package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamJobService;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.JobID;

/**
 * A production type used for generating one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L3ProductionType implements ProductionType {
    private final HadoopProcessingService processingService;
    private WpsXmlGenerator wpsXmlGenerator;
    private final L3ProcessingRequestFactory processingRequestFactory;

    L3ProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        this.processingService = processingService;
        wpsXmlGenerator = new WpsXmlGenerator();
        processingRequestFactory = new L3ProcessingRequestFactory(processingService, stagingService);
    }

    @Override
    public String getName() {
        return "calvalus-level3";
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        String productionId = Production.createId(productionRequest.getProductionType());
        String productionName = createL3ProductionName(productionRequest);

        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(productionId,
                                                                                                       "ewa", // todo - get user
                                                                                                       productionRequest);
        JobID[] jobIds = new JobID[l3ProcessingRequests.length];
        for (int i = 0; i < l3ProcessingRequests.length; i++) {
            String wpsXml = wpsXmlGenerator.createL3WpsXml(productionId, productionName, l3ProcessingRequests[i]);
            jobIds[i] = submitL3Job(wpsXml);
        }

        return new Production(productionId,
                              productionName,
                              "ewa", // todo - get user name
                              " ",
                              productionRequest,
                              jobIds);
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
    public L3Staging createStaging(Production hadoopProduction) throws ProductionException {
        JobClient jobClient = processingService.getJobClient();
        ProductionRequest productionRequest = hadoopProduction.getProductionRequest();
        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(hadoopProduction.getId(),
                                                                                                       hadoopProduction.getUser(),
                                                                                                       productionRequest);

        return new L3Staging(hadoopProduction, l3ProcessingRequests, jobClient.getConf());
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
