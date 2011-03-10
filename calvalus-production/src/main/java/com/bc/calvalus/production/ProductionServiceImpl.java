package com.bc.calvalus.production;


import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.esa.beam.util.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ProductionService implementation that delegates to a Hadoop cluster.
 * To use it, specify the servlet init-parameter 'calvalus.portal.backendService.class'
 * (context.xml or web.xml)
 */
public class ProductionServiceImpl implements ProductionService {

    private final ProcessingService processingService;
    private final StagingService stagingService;
    private final ProductionType[] productionTypes;
    private final ProductionStore productionStore;
    private final Map<String, Action> productionActionMap;
    private final Map<String, Staging> productionStagingsMap;
    private final Logger logger;
    private Timer statusObserver;

    public ProductionServiceImpl(ProcessingService processingService,
                                 StagingService stagingService,
                                 ProductionStore productionStore,
                                 ProductionType... productionTypes) throws ProductionException {
        this.productionStore = productionStore;
        this.processingService = processingService;
        this.stagingService = stagingService;
        this.productionTypes = productionTypes;
        this.productionActionMap = new HashMap<String, Action>();
        this.productionStagingsMap = new HashMap<String, Staging>();
        this.logger = Logger.getLogger("com.bc.calvalus");

        try {
            productionStore.load();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to load productions: " + e.getMessage(), e);
        }
    }

    public void startStatusObserver(int period) {
        statusObserver = new Timer("StatusObserver", true);
        statusObserver.scheduleAtFixedRate(new ProductionUpdater(), period, period);
    }

    public void stopStatusObserver() {
        statusObserver.cancel();
        statusObserver = null;
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws ProductionException {
        // todo - load & update from persistent storage
        return new ProductSet[]{
                new ProductSet("MER_RR__1P/r03/", "MERIS_RR__1P", "All MERIS RR L1b"),
                new ProductSet("MER_RR__1P/r03/2004", "MERIS_RR__1P", "MERIS RR L1b 2004"),
                new ProductSet("MER_RR__1P/r03/2005", "MERIS_RR__1P", "MERIS RR L1b 2005"),
                new ProductSet("MER_RR__1P/r03/2006", "MERIS_RR__1P", "MERIS RR L1b 2006"),
        };
    }

    @Override
    public ProcessorDescriptor[] getProcessors(String filter) throws ProductionException {
        // todo - load & update from persistent storage
        return new ProcessorDescriptor[]{
                new ProcessorDescriptor("CoastColour.L2W", "MERIS CoastColour",
                                        "<parameters>\n" +
                                                "  <useIdepix>true</useIdepix>\n" +
                                                "  <landExpression>l1_flags.LAND_OCEAN</landExpression>\n" +
                                                "  <outputReflec>false</outputReflec>\n" +
                                                "</parameters>",
                                        "beam-lkn",
                                        new String[]{"1.0-SNAPSHOT"}),
        };
    }

    @Override
    public synchronized Production[] getProductions(String filter) throws ProductionException {
        return productionStore.getProductions();
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest) throws ProductionException {
        ProductionType productionType = findProductionType(productionRequest);
        synchronized (this) {
            Production production = productionType.createProduction(productionRequest);
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
            throw new ProductionException(String.format("Only %d of %d production(s) have been staged.", count, productionIds.length));
        }
    }

    @Override
    public  void cancelProductions(String... productionIds) throws ProductionException {
        requestProductionKill(productionIds, Action.CANCEL);
    }

    @Override
    public  void deleteProductions(String... productionIds) throws ProductionException {
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
                    Object[] jobIds = production.getJobIds();
                    for (Object jobId : jobIds) {
                        try {
                            processingService.killJob(jobId);
                        } catch (IOException e) {
                            logger.log(Level.SEVERE, String.format("Failed to kill Hadoop job '%s' of production '%s': %s",
                                                                   jobId, production.getId(), e.getMessage()), e);
                        }
                    }
                }

                count++;
            } else {
                logger.warning(String.format("Failed to kill unknown production '%s'", productionId));
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(String.format("Only %d of %d production(s) have been killed. See server log for details.",
                                                        count, productionIds.length));
        }
    }

    private void stageProductionResults(Production production) throws ProductionException {
        production.setStagingStatus(ProcessStatus.SCHEDULED);
        ProductionType productionType = findProductionType(production.getProductionRequest());
        Staging staging = productionType.createStaging(production);
        productionStagingsMap.put(production.getId(), staging);
    }

    public void updateProductions() throws IOException, ProductionException {
        Map<Object, ProcessStatus> jobStatusMap = processingService.getJobStatusMap();
        Production[] productions = productionStore.getProductions();

        // Update state of all registered productions
        for (Production production : productions) {
            Object[] jobIds = production.getJobIds();
            ProcessStatus[] jobStatuses = new ProcessStatus[jobIds.length];
            for (int i = 0; i < jobIds.length; i++) {
                Object jobId = jobIds[i];
                ProcessStatus processStatus = jobStatusMap.get(jobId);
                jobStatuses[i] = processStatus != null ? processStatus : ProcessStatus.UNKNOWN;
            }
            production.setProcessingStatus(ProcessStatus.aggregate(jobStatuses));
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
                stageProductionResults(production);
            }
        }

        // write to persistent storage
        productionStore.store();
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
        productionStore.removeProduction(production);
        productionActionMap.remove(production.getId());
        productionStagingsMap.remove(production.getId());
        //TODO this should be done in the staging service (mz)
        File file = new File(stagingService.getStagingAreaPath(), production.getStagingPath());
        if (file.exists()) {
            SystemUtils.deleteFileTree(file);
        }
    }

    public static enum Action {
        CANCEL,
        DELETE,
        RESTART,// todo - implement restart
    }

    private class ProductionUpdater extends TimerTask {
        private long lastLog;

        @Override
        public void run() {
            try {
                updateProductions();
            } catch (Exception e) {
                logError(e);
            }
        }

        private void logError(Exception e) {
            long time = System.currentTimeMillis();
            // only log errors every 2 minutes
            if (time - lastLog > 120 * 1000L) {
                logger.log(Level.SEVERE, "Failed to update production state:" + e.getMessage(), e);
                lastLog = time;
            }
        }
    }
}
