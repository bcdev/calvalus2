package com.bc.calvalus.production;


import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.production.hadoop.HadoopProductionType;
import com.bc.calvalus.production.store.ProductionStore;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
public class ProductionServiceImpl implements ProductionService {

    public static enum Action {
        CANCEL,
        DELETE,
        RESTART,// todo - implement restart (nf)
    }

    private final InventoryService inventoryService;
    private final ProcessingService processingService;
    private final StagingService stagingService;
    private final ProductionType[] productionTypes;
    private final ProductionStore productionStore;
    private final Map<String, Action> productionActionMap;
    private final Map<String, Staging> productionStagingsMap;
    private final Logger logger;

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    public ProductionServiceImpl(InventoryService inventoryService,
                                 ProcessingService processingService,
                                 StagingService stagingService,
                                 ProductionStore productionStore,
                                 ProductionType... productionTypes) throws ProductionException {
        this.inventoryService = inventoryService;
        this.productionStore = productionStore;
        this.processingService = processingService;
        this.stagingService = stagingService;
        this.productionTypes = productionTypes;
        this.productionActionMap = new HashMap<String, Action>();
        this.productionStagingsMap = new HashMap<String, Staging>();
        this.logger = Logger.getLogger("com.bc.calvalus");
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws ProductionException {
        try {
            return inventoryService.getProductSets(filter);
        } catch (Exception e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public BundleDescriptor[] getBundles(BundleFilter filter) throws ProductionException {
        try {
            return processingService.getBundles(filter);
        } catch (Exception e) {
            throw new ProductionException("Failed to load list of processors.", e);
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
        logger.info("orderProduction:");
        logger.info("user: " + productionRequest.getUserName());
        logger.info("type: " + productionRequest.getProductionType());
        logger.info("parameters: " + productionRequest.getParameters());

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
            productionStore.addProduction(production);
            return new ProductionResponse(production);
        }
    }

    @Override
    public synchronized void stageProductions(String... productionIds) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            Production production = productionStore.getProduction(productionId);
            if (production != null) {
                try {
                    stageProductionResults(production);
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
    public void updateStatuses() {
        try {
            processingService.updateStatuses();
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

    @Override
    public String[] listUserFiles(String userName, String dirPath) throws ProductionException {
        try {
            String glob = getUserGlob(userName, dirPath);
            return inventoryService.globPaths(Arrays.asList(glob));
        } catch (IOException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public OutputStream addUserFile(String userName, String path) throws ProductionException {
        try {
            return inventoryService.addFile(getUserPath(userName, path));
        } catch (IOException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public boolean removeUserFile(String userName, String path) throws ProductionException {
        try {
            return inventoryService.removeFile(getUserPath(userName, path));
        } catch (IOException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public boolean removeUserDirectory(String userName, String path) throws ProductionException {
        try {
            return inventoryService.removeDirectory(getUserPath(userName, path));
        } catch (IOException e) {
            throw new ProductionException(e);
        }
    }

    @Override
    public String getQualifiedUserPath(String userName, String path) {
        return inventoryService.getQualifiedPath(getUserPath(userName, path));
    }

    private String getUserGlob(String userName, String dirPath) {
        return getUserPath(userName, dirPath) + "/.*";
    }

    private String getUserPath(String userName, String dirPath) {
        String path;
        if (dirPath.isEmpty() || "/".equals(dirPath)) {
            path = String.format("home/%s", userName.toLowerCase());
        } else {
            path = String.format("home/%s/%s", userName.toLowerCase(), dirPath);
        }
        return path;
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
        delteWorkflowOutput(production.getWorkflow());
        try {
            stagingService.deleteTree(production.getStagingPath());
        } catch (IOException e) {
            logger.log(Level.SEVERE, String.format("Failed to delete staging directory '%s' of production '%s': %s",
                                                   production.getStagingPath(), production.getId(), e.getMessage()), e);
        }
    }

    private void delteWorkflowOutput(WorkflowItem workflowItem) {
        if (workflowItem instanceof HadoopWorkflowItem) {
            HadoopWorkflowItem hadoopWorkflowItem = (HadoopWorkflowItem) workflowItem;
            try {
                JobUtils.clearDir(hadoopWorkflowItem.getOutputDir(), hadoopWorkflowItem.getJobConfig());
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed to delete output dir " + hadoopWorkflowItem.getOutputDir(), e);
            }
        } else {
            for (WorkflowItem item : workflowItem.getItems()) {
                delteWorkflowOutput(item);
            }
        }
    }
}
