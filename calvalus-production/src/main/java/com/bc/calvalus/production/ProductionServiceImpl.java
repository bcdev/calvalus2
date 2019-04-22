package com.bc.calvalus.production;


import com.bc.calvalus.commons.*;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.MaskDescriptor;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.production.hadoop.HadoopProductionType;
import com.bc.calvalus.production.store.ProductionStore;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
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

    public enum Action {
        CANCEL,
        DELETE,
        RESTART,// todo - implement restart (nf)
    }

    private final FileSystemService fileSystemService;
    private final ProcessingService processingService;
    private final StagingService stagingService;
    private final ProductionType[] productionTypes;
    private final ProductionStore productionStore;
    private final Map<String, Action> productionActionMap;
    private final Map<String, Staging> productionStagingsMap;
    private final Logger logger;

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    public ProductionServiceImpl(FileSystemService fileSystemService,
                                 ProcessingService processingService,
                                 StagingService stagingService,
                                 ProductionStore productionStore,
                                 ProductionType... productionTypes) throws ProductionException {
        this.fileSystemService = fileSystemService;
        this.productionStore = productionStore;
        this.processingService = processingService;
        this.stagingService = stagingService;
        this.productionTypes = productionTypes;
        this.productionActionMap = new HashMap<String, Action>();
        this.productionStagingsMap = new HashMap<String, Staging>();
        this.logger = CalvalusLogger.getLogger();
    }

    @Override
    public ProcessingService getProcessingService() {
        return processingService;
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
    public ProductionResponse orderProduction(ProductionRequest productionRequest) throws ProductionException {
        return orderProduction(productionRequest, null);
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest, HadoopJobHook jobHook) throws ProductionException {
        String user = productionRequest.getUserName();
        String type = productionRequest.getProductionType();
        logger.info("orderProduction: " + type + " (for " + user + ")");
        Map<String, String> parameters = productionRequest.getParameters();
        List<String> parametersKeys = new ArrayList<>(parameters.keySet());
        Collections.sort(parametersKeys);
        for (String key : parametersKeys) {
            logger.info(key + " = " + parameters.get(key));
        }

        synchronized (this) {
            Production production;
            try {
                ProductionType productionType = findProductionType(productionRequest);
                UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(user);
                production = remoteUser.doAs((PrivilegedExceptionAction<Production>) () ->
                    productionType.createProduction(productionRequest)
                );
                WorkflowItem workflow = production.getWorkflow();
                if (jobHook != null) {
                    injectJobHook(jobHook, workflow);
                }
                workflow.submit();
            } catch (Throwable t) {
                logger.log(Level.SEVERE, t.getMessage(), t);
                throw new ProductionException(String.format("Failed to submit production: %s", t.getMessage()), t);
            }
            productionStore.addProduction(production);
            return new ProductionResponse(production);
        }
    }

    private static void injectJobHook(HadoopJobHook jobHook, WorkflowItem workflowItem)  {
        if (workflowItem instanceof HadoopWorkflowItem) {
            HadoopWorkflowItem hadoopWorkflowItem = (HadoopWorkflowItem) workflowItem;
            hadoopWorkflowItem.setJobHook(jobHook);
        }
        for (WorkflowItem item : workflowItem.getItems()) {
            injectJobHook(jobHook, item);
        }
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
                            || production.getStagingStatus().getState() == ProcessState.CANCELLED)
                    {
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

    private synchronized void requestProductionKill(String[] productionIds, Action action) throws ProductionException {
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
    public synchronized void updateStatuses(String username) {
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
        if (outputDir == null  || outputDir.isEmpty()) {
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

    @Override
    public String[][] loadRegionDataInfo(String username, String url) throws IOException {
        return processingService.loadRegionDataInfo(username, url);
    }

    @Override
    public void invalidateBundleCache() {
        processingService.invalidateBundleCache();
    }
}
