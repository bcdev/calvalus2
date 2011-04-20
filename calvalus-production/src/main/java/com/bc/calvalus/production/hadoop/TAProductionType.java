package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.ta.TAConfig;
import com.bc.calvalus.processing.ta.TAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.util.Date;

/**
 * Trend analysis: A production type used for generating a time-series generated from L3 products and a numb er of
 * given regions.
 *
 * @author MarcoZ
 * @author Norman
 */
public class TAProductionType extends HadoopProductionType {

    public TAProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super("TA", processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createTAProductionName(productionRequest);

        String inputProductSetId = productionRequest.getParameterSafe("inputProductSetId");
        Date startDate = productionRequest.getDate("dateStart");
        Date stopDate = productionRequest.getDate("dateStop");  // todo - clarify meaning of this parameter (we use startDate + i * periodLength here)

        String processorName = productionRequest.getParameterSafe("processorName");
        String processorParameters = productionRequest.getParameterSafe("processorParameters");
        String processorBundle = String.format("%s-%s",
                                               productionRequest.getParameterSafe("processorBundleName"),
                                               productionRequest.getParameterSafe("processorBundleVersion"));

        Geometry roiGeometry = productionRequest.getRegionGeometry();

        L3Config l3Config = L3ProductionType.createL3Config(productionRequest);
        TAConfig taConfig = createTAConfig(productionRequest);

        int periodCount = Integer.parseInt(productionRequest.getParameter("periodCount"));
        int periodLength = Integer.parseInt(productionRequest.getParameter("periodLength")); // unit=days

        long time = startDate.getTime();
        long periodLengthMillis = periodLength * 24L * 60L * 60L * 1000L;

        Workflow.Parallel parallel = new Workflow.Parallel();
        for (int i = 0; i < periodCount; i++) {

            Date date1 = new Date(time);
            Date date2 = new Date(time + periodLengthMillis - 1L);

            String date1Str = ProductionRequest.getDateFormat().format(date1);
            String date2Str = ProductionRequest.getDateFormat().format(date2);

            Workflow.Sequential sequential = new Workflow.Sequential();

            String l3JobName = String.format("%s_%d_L3", productionId, (i + 1));
            String taJobName = String.format("%s_%d_TA", productionId, (i + 1));

            // todo - use geoRegion to filter input files
            String[] l1InputFiles = getInputFiles(inputProductSetId, date1, date2);
            String l3OutputDir = getOutputDir(productionRequest.getUserName(), l3JobName);
            String taOutputDir = getOutputDir(productionRequest.getUserName(), taJobName);

            L3WorkflowItem l3WorkflowItem = new L3WorkflowItem(getProcessingService(),
                                                               l3JobName,
                                                               processorBundle,
                                                               processorName,
                                                               processorParameters,
                                                               roiGeometry,
                                                               l1InputFiles,
                                                               l3OutputDir,
                                                               l3Config,
                                                               date1Str,
                                                               date2Str);

            TAWorkflowItem taWorkflowItem = new TAWorkflowItem(getProcessingService(),
                                                               taJobName,
                                                               l3OutputDir,
                                                               taOutputDir,
                                                               l3Config,
                                                               taConfig,
                                                               date1Str,
                                                               date2Str);

            sequential.add(l3WorkflowItem);
            sequential.add(taWorkflowItem);

            parallel.add(sequential);
            time += periodLengthMillis;
        }

        String stagingDir = String.format("%s/%s", productionRequest.getUserName(), productionId);
        return new Production(productionId,
                              productionName,
                              stagingDir,
                              productionRequest,
                              parallel);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        // todo - implement TAStaging
        return new L3Staging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    String getOutputDir(String userName, String dirName) {
        return String.format("%s/%s/%s",
                             getProcessingService().getDataOutputPath(),
                             userName,
                             dirName);
    }

    static String createTAProductionName(ProductionRequest productionRequest) {
        return String.format("Trend analysis using product set '%s' and L2 processor '%s'",
                             productionRequest.getParameter("inputProductSetId"),
                             productionRequest.getParameter("processorName"));

    }

    static TAConfig createTAConfig(ProductionRequest productionRequest) {
        GeometryFactory geometryFactory = new GeometryFactory();
        // todo - read requested regions from  productionRequest
        return new TAConfig(TAConfig.RegionConfiguration.GLOBE);
        // todo - add all required regions here
    }


}
