package com.bc.calvalus.production.hadoop;


import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionProcessor;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.mapreduce.JobID;

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
public class HadoopProductionService implements ProductionService {

    public static final File PRODUCTIONS_DB_FILE = new File("calvalus-productions-db.csv");
    private static final int HADOOP_OBSERVATION_PERIOD = 2000;

    private final ProcessingService processingService;
    private final HadoopProductionDatabase productionDatabase;
    private final Logger logger = Logger.getLogger("com.bc.calvalus");
    private final Map<String, ProductionType> productionTypeMap;

    public HadoopProductionService(ProcessingService processingService,
                                   ProductionType... productionTypes) throws ProductionException {

        this.processingService = processingService;
        this.productionDatabase = new HadoopProductionDatabase();  // todo - add as parameter
        this.productionTypeMap = new HashMap<String, ProductionType>();
        for (ProductionType productionType : productionTypes) {
            this.productionTypeMap.put(productionType.getName(), productionType);
        }

        loadProductions();

        Timer hadoopObservationTimer = new Timer(true);
        hadoopObservationTimer.scheduleAtFixedRate(new HadoopObservationTask(),
                                                   HADOOP_OBSERVATION_PERIOD, HADOOP_OBSERVATION_PERIOD);
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
    public ProductionProcessor[] getProcessors(String filter) throws ProductionException {
        // todo - load & update from persistent storage
        return new ProductionProcessor[]{
                new ProductionProcessor("CoastColour.L2W", "MERIS CoastColour",
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
    public Production[] getProductions(String filter) throws ProductionException {
        return productionDatabase.getProductions();
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest) throws ProductionException {
        ProductionType productionType = productionTypeMap.get(productionRequest.getProductionType());
        if (productionType == null) {
            throw new ProductionException(String.format("Unhandled production type '%s'",
                                                        productionRequest.getProductionType()));
        }
        HadoopProduction production = productionType.createProduction(productionRequest);
        productionDatabase.addProduction(production);
        return new ProductionResponse(production);
    }

    @Override
    public void stageProductions(String[] productionIds) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            HadoopProduction production = productionDatabase.getProduction(productionId);
            if (production != null) {
                try {
                    ProductionType productionType = productionTypeMap.get(production.getProductionRequest().getProductionType());
                    productionType.stageProduction(production);
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
    public void cancelProductions(String[] productionIds) throws ProductionException {
        requestProductionKill(productionIds, HadoopProduction.Action.CANCEL);
    }

    @Override
    public void deleteProductions(String[] productionIds) throws ProductionException {
        requestProductionKill(productionIds, HadoopProduction.Action.DELETE);
    }

    private void requestProductionKill(String[] productionIds, HadoopProduction.Action action) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            HadoopProduction production = productionDatabase.getProduction(productionId);
            if (production != null) {
                production.setAction(action);
                StagingJob stagingJob = production.getStagingJob();
                if (stagingJob != null && !stagingJob.isCancelled()) {
                    stagingJob.cancel();
                    production.setStagingJob(null);
                }

                IOException error = null;
                JobID[] jobIds = production.getJobIds();
                for (JobID jobId : jobIds) {
                    try {
                        processingService.killJob(jobId);
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, String.format("Failed to kill Hadoop job '%s' of production '%s': %s",
                                                               jobId, production.getId(), e.getMessage()), e);
                        error = error == null ? e : error;
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

    private void loadProductions() {
        System.out.println("PRODUCTIONS_DB_FILE = " + PRODUCTIONS_DB_FILE.getAbsolutePath());
        try {
            productionDatabase.load(PRODUCTIONS_DB_FILE);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to load productions: " + e.getMessage(), e);
        }
    }

    void updateProductionsState() throws IOException, ProductionException {
        Map<Object, ProductionStatus> jobStatusMap = processingService.getJobStatusMap();
        HadoopProduction[] productions = productionDatabase.getProductions();

        // Update state of all registered productions
        for (HadoopProduction production : productions) {
            JobID[] jobIds = production.getJobIds();
            ProductionStatus[] jobStatuses = new ProductionStatus[jobIds.length];
            for (int i = 0; i < jobIds.length; i++) {
                JobID jobId = jobIds[i];
                jobStatuses[i] = jobStatusMap.get(jobId);
            }
            production.setProcessingStatus(getProcessingStatus(jobStatuses));
        }

        // Now try to delete productions
        for (HadoopProduction production : productions) {
            if (HadoopProduction.Action.DELETE.equals(production.getAction())) {
                if (production.getProcessingStatus().isDone()) {
                    productionDatabase.removeProduction(production);
                }
            }
        }

        // Copy result to staging area
        for (HadoopProduction production : productions) {
            if (production.isOutputStaging()
                    && production.getProcessingStatus().getState() == ProductionState.COMPLETED
                    && production.getStagingStatus().getState() == ProductionState.WAITING
                    && production.getStagingJob() == null) {
                ProductionType productionType = productionTypeMap.get(production.getProductionRequest().getProductionType());
                productionType.stageProduction(production);
            }
        }

        // write to persistent storage
        productionDatabase.store(PRODUCTIONS_DB_FILE);
    }

    static ProductionStatus getProcessingStatus(ProductionStatus[] jobStatuses) {
        if (jobStatuses.length == 1) {
            return jobStatuses[0];
        } else if (jobStatuses.length > 1) {
            float progress = 0f;
            for (ProductionStatus jobStatus : jobStatuses) {
                progress += jobStatus.getProgress();
            }
            progress /= jobStatuses.length;
            for (ProductionStatus jobStatus : jobStatuses) {
                if (jobStatus.getState() == ProductionState.ERROR
                        || jobStatus.getState() == ProductionState.CANCELLED) {
                    return new ProductionStatus(jobStatus.getState(), progress, jobStatus.getMessage());
                }
            }

            int numCompleted = 0;
            int numWaiting = 0;
            for (ProductionStatus jobStatus : jobStatuses) {
                if (jobStatus.getState() == ProductionState.COMPLETED) {
                    numCompleted++;
                } else if (jobStatus.getState() == ProductionState.WAITING) {
                    numWaiting++;
                }
            }
            return new ProductionStatus(numCompleted == jobStatuses.length
                                                ? ProductionState.COMPLETED : numWaiting == jobStatuses.length
                    ? ProductionState.WAITING : ProductionState.IN_PROGRESS,
                                        progress);
        }
        return ProductionStatus.UNKNOWN;
    }

    private class HadoopObservationTask extends TimerTask {
        private long lastLog;

        @Override
        public void run() {
            try {
                updateProductionsState();
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
