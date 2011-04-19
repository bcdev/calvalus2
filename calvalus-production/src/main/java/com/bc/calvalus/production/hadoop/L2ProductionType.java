package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;

import java.util.Date;
import java.util.Map;

/**
 * A production type used for generating one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2ProductionType extends HadoopProductionType {

    public L2ProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super("calvalus-level2", processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createL2ProductionName(productionRequest);
        final String userName = productionRequest.getUserName();

        L2WorkflowItem workflowItem = createWorkflowItem(productionId, productionRequest);

        // todo - if autoStaging=true, create sequential workflow and add staging job
        boolean autoStaging = isAutoStaging(productionRequest);

        return new Production(productionId,
                              productionName,
                              userName + "/" + productionId,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new L2Staging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    static String createL2ProductionName(ProductionRequest productionRequest) {
        return String.format("Level 2 production using product set '%s' and L2 processor '%s'",
                             productionRequest.getProductionParameter("inputProductSetId"),
                             productionRequest.getProductionParameter("processorName"));
    }

    L2WorkflowItem createWorkflowItem(String productionId,
                                      ProductionRequest productionRequest) throws ProductionException {
        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        productionRequest.ensureProductionParameterSet("processorBundleName");
        productionRequest.ensureProductionParameterSet("processorBundleVersion");
        productionRequest.ensureProductionParameterSet("processorName");
        productionRequest.ensureProductionParameterSet("processorParameters");
        productionRequest.ensureProductionParameterSet("dateStart");
        productionRequest.ensureProductionParameterSet("dateStop");

        String inputProductSetId = productionRequest.getProductionParameterSafe("inputProductSetId");
        Date startDate = productionRequest.getDate("dateStart");
        Date stopDate = productionRequest.getDate("dateStop");

        Geometry roiGeometry = productionRequest.getRoiGeometry();
        // todo - use geoRegion to filter input files
        String[] inputFiles = getInputFiles(inputProductSetId, startDate, stopDate);
        String outputDir = getOutputDir(productionId, productionRequest);

        String processorBundle = String.format("%s-%s",
                                               productionParameters.get("processorBundleName"),
                                               productionParameters.get("processorBundleVersion"));
        String processorName = productionParameters.get("processorName");
        String processorParameters = productionParameters.get("processorParameters");

        return new L2WorkflowItem(getProcessingService(),
                                  productionId,
                                  processorBundle,
                                  processorName,
                                  processorParameters,
                                  roiGeometry,
                                  inputFiles,
                                  outputDir
        );
    }

    String getOutputDir(String productionId, ProductionRequest productionRequest) {
        return getProcessingService().getDataOutputPath() + "/" + productionRequest.getUserName() + "/" + productionId;
    }
}
