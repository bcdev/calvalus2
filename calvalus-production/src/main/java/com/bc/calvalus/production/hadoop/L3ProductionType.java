package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamJobService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.JobID;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A production type used for generating one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
class L3ProductionType implements ProductionType {
    private final HadoopProcessingService processingService;
    private WpsXmlGenerator wpsXmlGenerator;
    private final ExecutorService stagingService;
    private File localStagingDir;
    private final L3ProcessingRequestFactory processingRequestFactory;

    L3ProductionType(HadoopProcessingService processingService, File localStagingDir) throws ProductionException {
        this.processingService = processingService;
        this.localStagingDir = localStagingDir;
        wpsXmlGenerator = new WpsXmlGenerator();
        stagingService = Executors.newFixedThreadPool(3); // todo - make numThreads configurable
        processingRequestFactory = new L3ProcessingRequestFactory(processingService, localStagingDir);
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
            JobClient jobClient = processingService.getJobClient();
            BeamJobService beamJobService = new BeamJobService(jobClient);
            return beamJobService.submitJob(wpsXml);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ProductionException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    @Override
    public void stageProduction(HadoopProduction hadoopProduction) throws ProductionException {
        JobClient jobClient = processingService.getJobClient();
        ProductionRequest productionRequest = hadoopProduction.getProductionRequest();
        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(productionRequest);
        L3StagingJob l3StagingJob = new L3StagingJob(hadoopProduction, l3ProcessingRequests, jobClient.getConf(), localStagingDir.getPath());
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
