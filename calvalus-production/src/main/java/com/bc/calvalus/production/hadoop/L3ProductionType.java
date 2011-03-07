package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamJobService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.JobID;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * A production type used for generating one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
class L3ProductionType implements ProductionType {
    private WpsXmlGenerator wpsXmlGenerator;
    private final ExecutorService stagingService;
    private final JobClient jobClient;
    private File localStagingDir;
    private Logger logger;
    private final L3ProcessingRequestFactory processingRequestFactory;

    L3ProductionType(JobClient jobClient, File localStagingDir, Logger logger) throws ProductionException {
        this.logger = logger;
        this.localStagingDir = localStagingDir;
        this.jobClient = jobClient;
        wpsXmlGenerator = new WpsXmlGenerator();
        stagingService = Executors.newFixedThreadPool(3); // todo - make numThreads configurable
        processingRequestFactory = new HadoopL3ProcessingRequestFactory(jobClient, localStagingDir);
    }

    @Override
    public String getName() {
        return "calvalus-level3";
    }

    @Override
    public HadoopProduction createProduction(ProductionRequest productionRequest) throws ProductionException {

        String l3ProductionId = createL3ProductionId(productionRequest);
        String l3ProductionName = createL3ProductionName(productionRequest);

        boolean staging = Boolean.parseBoolean(productionRequest.getProductionParameter("outputStaging"));
        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(productionRequest);
        JobID[] jobIds = new JobID[l3ProcessingRequests.length];
        for (int i = 0; i < l3ProcessingRequests.length; i++) {
            String wpsXml = wpsXmlGenerator.createL3WpsXml(l3ProductionId, l3ProductionName, l3ProcessingRequests[i]);
            jobIds[i] = submitL3Job(wpsXml);
        }

        return new HadoopProduction(l3ProductionId,
                                    l3ProductionName,
                                    staging, jobIds,
                                    productionRequest);
    }

    private JobID submitL3Job(String wpsXml) throws ProductionException {
        try {
            BeamJobService beamJobService = new BeamJobService(jobClient);
            return beamJobService.submitJob(wpsXml);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ProductionException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    @Override
    public void stageProduction(HadoopProduction hadoopProduction) throws ProductionException {
        ProductionRequest productionRequest = hadoopProduction.getProductionRequest();

        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(productionRequest);

        L3StagingJob l3StagingJob = new L3StagingJob(hadoopProduction, l3ProcessingRequests, jobClient.getConf(), localStagingDir.getPath(), logger);
        stagingService.submit(l3StagingJob);
        hadoopProduction.setStagingJob(l3StagingJob);
    }

    static String createL3ProductionId(ProductionRequest productionRequest) {
        return productionRequest.getProductionType() + "-" + Long.toHexString(System.nanoTime());

    }

    static String createL3ProductionName(ProductionRequest productionRequest) {
        return String.format("Level 3 production using product set '%s' and L2 processor '%s'",
                             productionRequest.getProductionParameter("inputProductSetId"),
                             productionRequest.getProductionParameter("l2ProcessorName"));

    }
}
