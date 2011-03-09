package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamJobService;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.JobID;

import java.text.SimpleDateFormat;
import java.util.Date;

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
    private static final SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");

    L3ProductionType(HadoopProcessingService processingService, String localStagingDir) throws ProductionException {
        this.processingService = processingService;
        wpsXmlGenerator = new WpsXmlGenerator();
        processingRequestFactory = new L3ProcessingRequestFactory(processingService, localStagingDir);
    }

    @Override
    public String getName() {
        return "calvalus-level3";
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        String l3ProductionId = createL3ProductionId(productionRequest);
        String l3ProductionName = createL3ProductionName(productionRequest);

        boolean staging = Boolean.parseBoolean(productionRequest.getProductionParameter("outputStaging"));
        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(l3ProductionId,
                                                                                                       "ewa", // todo - get user
                                                                                                       productionRequest);
        JobID[] jobIds = new JobID[l3ProcessingRequests.length];
        for (int i = 0; i < l3ProcessingRequests.length; i++) {
            String wpsXml = wpsXmlGenerator.createL3WpsXml(l3ProductionId, l3ProductionName, l3ProcessingRequests[i]);
            jobIds[i] = submitL3Job(wpsXml);
        }

        return new Production(l3ProductionId,
                              l3ProductionName,
                              "ewa", // todo - get user name
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
    public L3Staging createStaging(Production hadoopProduction) throws ProductionException {
        JobClient jobClient = processingService.getJobClient();
        ProductionRequest productionRequest = hadoopProduction.getProductionRequest();
        L3ProcessingRequest[] l3ProcessingRequests = processingRequestFactory.createProcessingRequests(hadoopProduction.getId(),
                                                                                                       hadoopProduction.getUser(),
                                                                                                       productionRequest);

        return new L3Staging(hadoopProduction, l3ProcessingRequests, jobClient.getConf());
    }

    static String createL3ProductionId(ProductionRequest productionRequest) {
        return String.format("%s_%s_%s", yyyyMMddHHmmss.format(new Date()),
                             productionRequest.getProductionType(),
                             Long.toHexString(System.nanoTime()));

    }

    static String createL3ProductionName(ProductionRequest productionRequest) {
        return String.format("Level 3 production using product set '%s' and L2 processor '%s'",
                             productionRequest.getProductionParameter("inputProductSetId"),
                             productionRequest.getProductionParameter("l2ProcessorName"));

    }
}
