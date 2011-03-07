package com.bc.calvalus.production.hadoop;


import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionProcessor;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionState;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
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
    private final JobClient jobClient;
    private final HadoopProductionDatabase database;
    // todo - Persist
    private final Logger logger;
    private final Map<String, ProductionType> productionTypeMap;

    public HadoopProductionService(JobClient jobClient, Logger logger,
                                   ProductionType... productionTypes) throws ProductionException {
        this.logger = logger;
        this.jobClient = jobClient;
        this.database = new HadoopProductionDatabase();

        initDatabase();

        Timer hadoopObservationTimer = new Timer(true);
        hadoopObservationTimer.scheduleAtFixedRate(new HadoopObservationTask(),
                                                   HADOOP_OBSERVATION_PERIOD, HADOOP_OBSERVATION_PERIOD);


        productionTypeMap = new HashMap<String, ProductionType>();
        for (ProductionType productionType : productionTypes) {
            productionTypeMap.put(productionType.getName(), productionType);
        }
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
        return database.getProductions();
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest) throws ProductionException {
        ProductionType productionType = productionTypeMap.get(productionRequest.getProductionType());
        if (productionType == null) {
            throw new ProductionException(String.format("Unhandled production type '%s'",
                                                        productionRequest.getProductionType()));
        }
        HadoopProduction production = productionType.createProduction(productionRequest);
        database.addProduction(production);
        return new ProductionResponse(production);
    }

    @Override
    public void stageProductions(String[] productionIds) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            HadoopProduction production = database.getProduction(productionId);
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
            HadoopProduction production = database.getProduction(productionId);
            if (production != null) {
                production.setAction(action);
                try {
                    StagingJob stagingJob = production.getStagingJob();
                    if (stagingJob != null && !stagingJob.isCancelled()) {
                        stagingJob.cancel();
                        production.setStagingJob(null);
                    }

                    JobID jobId = production.getJobId();
                    org.apache.hadoop.mapred.JobID oldJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
                    RunningJob runningJob = jobClient.getJob(oldJobId);
                    if (runningJob != null) {
                        runningJob.killJob();
                        count++;
                    }
                } catch (IOException e) {
                    logger.log(Level.SEVERE, String.format("Failed to cancel production '%s': %s",
                                                           production.getId(), e.getMessage()), e);
                }
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(String.format("Only %d of %d production(s) have been deleted.",
                                                        count, productionIds.length));
        }
    }

    private void initDatabase() {
        System.out.println("PRODUCTIONS_DB_FILE = " + PRODUCTIONS_DB_FILE.getAbsolutePath());
        try {
            database.load(PRODUCTIONS_DB_FILE);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to load productions: " + e.getMessage(), e);
        }
    }


    private Map<JobID, JobStatus> getJobStatusMap() throws IOException {
        JobStatus[] jobStatuses = jobClient.getAllJobs();
        HashMap<JobID, JobStatus> jobStatusMap = new HashMap<JobID, JobStatus>();
        for (JobStatus jobStatus : jobStatuses) {
            jobStatusMap.put(jobStatus.getJobID(), jobStatus);
        }
        return jobStatusMap;
    }

    private void updateProductionsState() throws IOException, ProductionException {
        Map<JobID, JobStatus> jobStatusMap = getJobStatusMap();
        HadoopProduction[] productions = database.getProductions();

        // Update state of all registered productions
        for (HadoopProduction production : productions) {
            JobStatus jobStatus = jobStatusMap.get(production.getJobId());
            production.setProductionStatus(jobStatus);
        }

        // Now try to delete productions
        for (HadoopProduction production : productions) {
            if (HadoopProduction.Action.DELETE.equals(production.getAction())) {
                if (production.getProcessingStatus().isDone()) {
                    database.removeProduction(production);
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
        database.store(PRODUCTIONS_DB_FILE);
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
