package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamOpProcessingType;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
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

        String productionId = Production.createId(productionRequest.getProductionType());
        String productionName = createL3ProductionName(productionRequest);
        String userName = "ewa";  // todo - get user from productionRequest

        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(productionId,
                                                                                                       userName,
                                                                                                       productionRequest);
        JobID[] jobIds = new JobID[l3ProcessingRequests.length];
        for (int i = 0; i < l3ProcessingRequests.length; i++) {
            String wpsXml = wpsXmlGenerator.createL3WpsXml(productionId, productionName, l3ProcessingRequests[i]);
            jobIds[i] = submitL3Job(wpsXml);
        }

        return new Production(productionId,
                              productionName,
                              userName,
                              userName + "/" + productionId,
                              productionRequest,
                              jobIds);
    }

    private JobID submitL3Job(String wpsXml) throws ProductionException {
        try {
            JobClient jobClient = processingService.getJobClient();
            BeamOpProcessingType beamOpProcessingType = new BeamOpProcessingType(jobClient);
            return beamOpProcessingType.submitJob(wpsXml);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ProductionException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    @Override
    public L3Staging createStaging(Production hadoopProduction) throws ProductionException {
        JobClient jobClient = processingService.getJobClient();
        ProductionRequest productionRequest = hadoopProduction.getProductionRequest();
        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(hadoopProduction.getId(),
                                                                                                       hadoopProduction.getUser(),
                                                                                                       productionRequest);
        L3Staging l3Staging = new L3Staging(hadoopProduction, l3ProcessingRequests, jobClient.getConf(), stagingService.getStagingAreaPath());
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
