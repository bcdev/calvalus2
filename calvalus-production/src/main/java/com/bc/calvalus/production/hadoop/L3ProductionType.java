package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.beam.BeamOpProcessingType;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.AbstractWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.production.Workflow;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;

/**
 * A production type used for generating one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L3ProductionType implements ProductionType {
    private final HadoopProcessingService processingService;
    private final StagingService stagingService;
    private WpsXmlGenerator wpsXmlGenerator;
    private final L3ProcessingRequestFactory processingRequestFactory;

    L3ProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        this.processingService = processingService;
        this.stagingService = stagingService;
        wpsXmlGenerator = new WpsXmlGenerator();
        processingRequestFactory = new L3ProcessingRequestFactory(processingService);
    }

    @Override
    public String getName() {
        return "calvalus-level3";
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createL3ProductionName(productionRequest);
        final String userName = "ewa";  // todo - get user from productionRequest

        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(productionId,
                                                                                                       userName,
                                                                                                       productionRequest);
        // todo - WORKFLOW {{{
        Workflow.Parallel parallel = new Workflow.Parallel();
        for (L3ProcessingRequest l3ProcessingRequest : l3ProcessingRequests) {
            final L3ProcessingRequest pcr = l3ProcessingRequest;
            parallel.add(new AbstractWorkflowItem() {
                JobID jobId;

                @Override
                public void submit() throws ProductionException {
                    try {
                        String wpsXml = wpsXmlGenerator.createL3WpsXml(productionId, productionName, pcr);
                        JobClient jobClient = processingService.getJobClient();
                        BeamOpProcessingType beamOpProcessingType = new BeamOpProcessingType(jobClient);
                        jobId = beamOpProcessingType.submitJob(wpsXml);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new ProductionException("Failed to submit Hadoop job: " + e.getMessage(), e);
                    }
                }

                @Override
                public void kill() throws ProductionException {
                    try {
                        processingService.killJob(jobId);
                    } catch (IOException e) {
                        throw new ProductionException("Failed to kill Hadoop job: " + e.getMessage(), e);
                    }
                }

                @Override
                public void updateStatus() {
                    if (jobId != null) {
                        ProcessStatus jobStatus = processingService.getJobStatus(jobId);
                        if (jobStatus != null) {
                            setStatus(jobStatus);
                        }
                    }
                }

                @Override
                public Object[] getJobIds() {
                    return jobId != null ? new Object[]{jobId} : new Object[0];
                }
            });
        }
        // todo - }}} WORKFLOW

        return new Production(productionId,
                              productionName,
                              userName,
                              userName + "/" + productionId,
                              productionRequest,
                              parallel);
    }

    @Override
    public L3Staging createStaging(Production hadoopProduction) throws ProductionException {
        JobClient jobClient = processingService.getJobClient();
        ProductionRequest productionRequest = hadoopProduction.getProductionRequest();
        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(hadoopProduction.getId(),
                                                                                                       hadoopProduction.getUser(),
                                                                                                       productionRequest);
        L3Staging l3Staging = new L3Staging(hadoopProduction, l3ProcessingRequests, jobClient.getConf(), stagingService.getStagingDir());
        try {
            stagingService.submitStaging(l3Staging);
        } catch (IOException e) {
            throw new ProductionException(String.format("Failed to order staging for production '%s': %s",
                                                        hadoopProduction.getId(), e.getMessage()), e);
        }
        return l3Staging;
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return getName().equalsIgnoreCase(productionRequest.getProductionType());
    }

    static String createL3ProductionName(ProductionRequest productionRequest) {
        return String.format("Level 3 production using product set '%s' and L2 processor '%s'",
                             productionRequest.getProductionParameter("inputProductSetId"),
                             productionRequest.getProductionParameter("l2ProcessorName"));

    }
}
