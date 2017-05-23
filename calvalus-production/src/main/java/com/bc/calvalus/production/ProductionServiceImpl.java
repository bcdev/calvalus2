package com.bc.calvalus.production;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.MaskDescriptor;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.hadoop.HadoopProductionType;
import com.bc.calvalus.production.store.ProductionStore;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.commons.codec.binary.Hex;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A ProductionService implementation that delegates to a Hadoop cluster.
 * To use it, specify the servlet init-parameter 'calvalus.portal.backendService.class'
 * (context.xml or web.xml)
 */
public class ProductionServiceImpl extends Observable implements ProductionService {

    public static final String SEPARATOR = "<bc-encryption-separator>";

    public static enum Action {
        CANCEL,
        DELETE,
        RESTART,// todo - implement restart (nf)
    }

    private final boolean isSamlAuthentication;
    private final FileSystemService fileSystemService;
    private final ProcessingService processingService;
    private final StagingService stagingService;
    private final ProductionType[] productionTypes;
    private final ProductionStore productionStore;
    private final Map<String, Action> productionActionMap;
    private final Map<String, Staging> productionStagingsMap;
    private final Logger logger;

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    public ProductionServiceImpl(boolean isSamlAuthentication,
                                 FileSystemService fileSystemService,
                                 ProcessingService processingService,
                                 StagingService stagingService,
                                 ProductionStore productionStore,
                                 ProductionType... productionTypes) throws ProductionException {
        this.isSamlAuthentication = isSamlAuthentication;
        this.fileSystemService = fileSystemService;
        this.productionStore = productionStore;
        this.processingService = processingService;
        this.stagingService = stagingService;
        this.productionTypes = productionTypes;
        this.productionActionMap = new HashMap<>();
        this.productionStagingsMap = new HashMap<>();
        this.logger = CalvalusLogger.getLogger();
    }

    @Override
    public BundleDescriptor[] getBundles(String username, BundleFilter filter) throws ProductionException {
        try {
            return processingService.getBundles(username, filter);
        } catch (Exception e) {
            throw new ProductionException("Failed to load list of processors.", e);
        }
    }

    @Override
    public MaskDescriptor[] getMasks(String userName) throws ProductionException {
        try {
            return processingService.getMasks(userName);
        } catch (Exception e) {
            throw new ProductionException("Failed to load list of masks.", e);
        }
    }

    @Override
    public synchronized Production[] getProductions(String filter) throws ProductionException {
        return productionStore.getProductions();
    }

    @Override
    public Production getProduction(String id) throws ProductionException {
        return productionStore.getProduction(id);
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest, String encryptedSamlToken) throws ProductionException {
        String user = productionRequest.getUserName();
        String type = productionRequest.getProductionType();
        logger.info("orderProduction: " + type + " (for " + user + ")");
        Map<String, String> parameters = productionRequest.getParameters();
        List<String> parametersKeys = new ArrayList<>(parameters.keySet());
        Collections.sort(parametersKeys);
        for (String key : parametersKeys) {
            logger.info(key + " = " + parameters.get(key));
        }

        if (isSamlAuthentication) {
            if (encryptedSamlToken == null) {
                throw new ProductionException("SAML authentication necessary but no SAML token received.");
            }

            String authToken = new StringBuilder()
                    .append(encryptedSamlToken)
                    .append(SEPARATOR)
                    .append(productionRequest.getUserName())
                    .append(SEPARATOR)
                    .append(productionRequest.getRegionName())
                    .append(SEPARATOR)
                    .append(productionRequest.getProductionType())
                    .toString();
            String encryptedAuthToken;
            try {
                encryptedAuthToken = encrypt(authToken);
            } catch (Exception e) {
                throw new ProductionException(e);
            }
            productionRequest.setAuthToken(encryptedAuthToken);
        }

        synchronized (this) {
            Production production;
            try {
                ProductionType productionType = findProductionType(productionRequest);
                production = productionType.createProduction(productionRequest);
                production.getWorkflow().submit();
            } catch (Throwable t) {
                logger.log(Level.SEVERE, t.getMessage(), t);
                throw new ProductionException(String.format("Failed to submit production: %s", t.getMessage()), t);
            }
            production.getProductionRequest().removeAuthToken();
            productionStore.addProduction(production);
            return new ProductionResponse(production);
        }
    }

    //    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest) throws ProductionException {
        return orderProduction(productionRequest, null);
    }

    @Override
    public synchronized void stageProductions(String... productionIds) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            Production production = productionStore.getProduction(productionId);
            if (production != null) {
                try {
                    if (production.getProcessingStatus().getState() == ProcessState.COMPLETED
                            && ((production.getStagingStatus().getState() == ProcessState.UNKNOWN
                            && productionStagingsMap.get(production.getId()) == null))
                            || production.getStagingStatus().getState() == ProcessState.ERROR
                            || production.getStagingStatus().getState() == ProcessState.CANCELLED) {
                        stageProductionResults(production);
                    }
                    count++;
                } catch (ProductionException e) {
                    logger.log(Level.SEVERE, String.format("Failed to stage production '%s': %s",
                            production.getId(), e.getMessage()), e);
                }
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(
                    String.format("Only %d of %d production(s) have been staged.", count, productionIds.length));
        }
    }


    @Override
    public synchronized void scpProduction(final String productionId, final String scpPath) throws ProductionException {

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    ScpTo scpTo = null;
                    try {

                        Pattern pattern = Pattern.compile("(.*)@(.*):(.*)");
                        Matcher matcher = pattern.matcher(scpPath);
                        if (!matcher.find()) {
                            throw new ProductionException("Could not parse scpPath!");
                        }
                        String user = matcher.group(1);
                        String host = matcher.group(2);
                        String remoteFilePath = matcher.group(3);

                        scpTo = new ScpTo(user, host);
                        scpTo.connect();
                        final Production production = getProduction(productionId);
                        ProductionType type = findProductionType(production.getProductionRequest());
                        if (!(type instanceof HadoopProductionType)) {
                            return;
                        }
                        HadoopProductionType hadoopProductionType = (HadoopProductionType) type;
                        File stagingBaseDir = hadoopProductionType.getStagingService().getStagingDir();
                        final File inputDir = new File(stagingBaseDir, production.getStagingPath());

                        File[] listToCopy = inputDir.listFiles(new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                final String zippedProductionFilename = ProductionStaging.getSafeFilename(production.getName() + ".zip");
                                return !name.equals(zippedProductionFilename);
                            }
                        });
                        logger.info("Starting to copy via scp");
                        if (listToCopy != null) {
                            logger.info("Copying " + listToCopy.length + " file(s)");
                            int tenthPart = listToCopy.length / 10;
                            for (int i = 0; i < listToCopy.length; i++) {
                                File file = listToCopy[i];
                                scpTo.copy(file.getCanonicalPath(), remoteFilePath);
                                if (i % tenthPart == 0) {
                                    logger.info("Copied " + i + " of " + listToCopy.length + " file(s)");
                                }
                            }

                        }
                    } finally {
                        if (scpTo != null) {
                            scpTo.disconnect();
                        }
                        logger.info("Finished copying via scp");

                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        });

    }

    @Override
    public void cancelProductions(String... productionIds) throws ProductionException {
        requestProductionKill(productionIds, Action.CANCEL);
    }

    @Override
    public void deleteProductions(String... productionIds) throws ProductionException {
        requestProductionKill(productionIds, Action.DELETE);
    }

    private void requestProductionKill(String[] productionIds, Action action) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            Production production = productionStore.getProduction(productionId);
            if (production != null) {
                productionActionMap.put(production.getId(), action);

                Staging staging = productionStagingsMap.get(production.getId());
                if (staging != null && !staging.isCancelled()) {
                    productionStagingsMap.remove(production.getId());
                    staging.cancel();
                }

                if (production.getProcessingStatus().isDone()) {
                    if (action == Action.DELETE) {
                        removeProduction(production);
                    }
                } else {
                    try {
                        production.getWorkflow().kill();
                    } catch (WorkflowException e) {
                        logger.log(Level.SEVERE, String.format("Failed to kill production '%s': %s",
                                production.getId(), e.getMessage()), e);
                    }
                }

                count++;
            } else {
                logger.warning(String.format("Failed to kill unknown production '%s'", productionId));
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(
                    String.format("Only %d of %d production(s) have been killed. See server log for details.",
                            count, productionIds.length));
        }
    }

    private void stageProductionResults(Production production) throws ProductionException {
        production.setStagingStatus(ProcessStatus.SCHEDULED);
        ProductionType productionType = findProductionType(production.getProductionRequest());
        Staging staging = productionType.createStaging(production);
        productionStagingsMap.put(production.getId(), staging);
    }

    @Override
    public void updateStatuses(String username) {
        try {
            processingService.updateStatuses(username);
        } catch (Exception e) {
            logger.warning("Failed to update job statuses: " + e.getMessage());
        }

        // Update state of all registered productions
        Production[] productions = productionStore.getProductions();
        for (Production production : productions) {
            production.getWorkflow().updateStatus();
        }

        // Now try to delete productions
        for (Production production : productions) {
            if (production.getProcessingStatus().isDone()) {
                Action action = productionActionMap.get(production.getId());
                if (action == Action.DELETE) {
                    removeProduction(production);
                }
            }
        }

        // Copy result to staging area
        for (Production production : productions) {
            if (production.isAutoStaging()
                    && production.getProcessingStatus().getState() == ProcessState.COMPLETED
                    && production.getStagingStatus().getState() == ProcessState.UNKNOWN
                    && productionStagingsMap.get(production.getId()) == null) {
                try {
                    stageProductionResults(production);
                } catch (ProductionException e) {
                    logger.warning("Failed to stage production: " + e.getMessage());
                }
            }
        }

        // write changes to persistent storage
        try {
            productionStore.persist();
            // logger.info("Production store persisted.");
        } catch (ProductionException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    @Override
    public void close() throws ProductionException {
        try {
            try {
                stagingService.close();
                executorService.shutdown();
            } finally {
                try {
                    processingService.close();
                } finally {
                    productionStore.close();
                }
            }
        } catch (Exception e) {
            throw new ProductionException("Failed to close production service: " + e.getMessage(), e);
        }
    }

    private ProductionType findProductionType(ProductionRequest productionRequest) throws ProductionException {
        for (ProductionType productionType : productionTypes) {
            if (productionType.getName().equals(productionRequest.getProductionType())) {
                return productionType;
            }
        }
        for (ProductionType productionType : productionTypes) {
            if (productionType.accepts(productionRequest)) {
                return productionType;
            }
        }
        throw new ProductionException(String.format("Unhandled production request of type '%s'",
                productionRequest.getProductionType()));
    }

    private synchronized void removeProduction(Production production) {
        productionStore.removeProduction(production.getId());
        productionActionMap.remove(production.getId());
        productionStagingsMap.remove(production.getId());

        String userName = production.getProductionRequest().getUserName();
        deleteOutput(production.getOutputPath(), userName);
        for (String dir : production.getIntermediateDataPath()) {
            deleteOutput(dir, userName);
        }
        try {
            stagingService.deleteTree(production.getStagingPath());
        } catch (IOException e) {
            logger.log(Level.SEVERE, String.format("Failed to delete staging directory '%s' of production '%s': %s",
                    production.getStagingPath(), production.getId(), e.getMessage()), e);
        }
    }

    private void deleteOutput(String outputDir, String userName) {
        if (outputDir == null || outputDir.isEmpty()) {
            return;
        }
        try {
            fileSystemService.removeDirectory(userName, outputDir);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to delete output dir " + outputDir, e);
        }
    }

    // TODO: race condition! implement Observable without "changed"
    @Override
    public synchronized void setChanged() {
        super.setChanged();
    }


    private static PublicKey getPublicKey() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        // todo - read public key correctly
        byte[] keyBytes = Files.readAllBytes(Paths.get("d:\\workspace\\code\\testkey\\public_key.der"));

        X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);

        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(spec);
    }

    public static String encrypt(String plaintext) throws Exception {
        Cipher encrypt = Cipher.getInstance("RSA");
        encrypt.init(Cipher.ENCRYPT_MODE, getPublicKey());
        byte[] bytes = plaintext.getBytes("UTF-8");

        byte[] encrypted = blockCipher(encrypt, bytes, Cipher.ENCRYPT_MODE);

        char[] encryptedTranspherable = Hex.encodeHex(encrypted);
        return new String(encryptedTranspherable);
    }

    private static byte[] blockCipher(Cipher cipher, byte[] bytes, int mode) throws IllegalBlockSizeException, BadPaddingException {
        // string initialize 2 buffers.
        // scrambled will hold intermediate results
        byte[] scrambled;

        // toReturn will hold the total result
        byte[] toReturn = new byte[0];
        // if we encrypt we use 100 byte long blocks. Decryption requires 128 byte long blocks (because of RSA)
        int length = (mode == Cipher.ENCRYPT_MODE) ? 100 : 128;

        // another buffer. this one will hold the bytes that have to be modified in this step
        byte[] buffer = new byte[length];

        for (int i = 0; i < bytes.length; i++) {

            // if we filled our buffer array we have our block ready for de- or encryption
            if ((i > 0) && (i % length == 0)) {
                //execute the operation
                scrambled = cipher.doFinal(buffer);
                // add the result to our total result.
                toReturn = append(toReturn, scrambled);
                // here we calculate the length of the next buffer required
                int newlength = length;

                // if newlength would be longer than remaining bytes in the bytes array we shorten it.
                if (i + length > bytes.length) {
                    newlength = bytes.length - i;
                }
                // clean the buffer array
                buffer = new byte[newlength];
            }
            // copy byte into our buffer.
            buffer[i % length] = bytes[i];
        }

        // this step is needed if we had a trailing buffer. should only happen when encrypting.
        // example: we encrypt 110 bytes. 100 bytes per run means we "forgot" the last 10 bytes. they are in the buffer array
        scrambled = cipher.doFinal(buffer);

        // final step before we can return the modified data.
        toReturn = append(toReturn, scrambled);

        return toReturn;
    }

    private static byte[] append(byte[] prefix, byte[] suffix) {
        byte[] toReturn = new byte[prefix.length + suffix.length];
        System.arraycopy(prefix, 0, toReturn, 0, prefix.length);
        System.arraycopy(suffix, 0, toReturn, prefix.length, suffix.length);
        return toReturn;
    }

}
