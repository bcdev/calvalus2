package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2ProductionType extends HadoopProductionType {

    static final String NAME = "L2";

    public L2ProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProdcutionName(
                createProductionName("Level 2 ", productionRequest));

        L2WorkflowItem workflowItem = createWorkflowItem(productionId, productionName, productionRequest);

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              workflowItem.getOutputDir(),
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new L2Staging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    L2WorkflowItem createWorkflowItem(String productionId,
                                      String productionName,
                                      ProductionRequest productionRequest) throws ProductionException {

        productionRequest.ensureParameterSet("processorName");
        List<DateRange> dateRanges = productionRequest.getDateRanges();

        String outputDir = getOutputPath(productionRequest, productionId, "");

        Configuration l2JobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(l2JobConfig, processorProductionRequest);
        setRequestParameters(l2JobConfig, productionRequest);

        Geometry geometry = productionRequest.getRegionGeometry(null);
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, geometry != null ? geometry.toString() : "");

        Date startDate = dateRanges.get(0).getStartDate();
        Date stopDate = dateRanges.get(dateRanges.size() - 1).getStopDate();
        String pathPattern = outputDir + "/.*${yyyy}${MM}${dd}.*.seq$";
        String regionWKT = geometry != null ? geometry.toString() : null;
        ProcessorDescriptor processorDescriptor = processorProductionRequest.getProcessorDescriptor(
                getProcessingService());
        ProductSet productSet = new ProductSet(getResultingProductionType(processorDescriptor),
                                               productionName, pathPattern, startDate, stopDate,
                                               productionRequest.getRegionName(), regionWKT);

        L2WorkflowItem l2WorkflowItem = new L2WorkflowItem(getProcessingService(), productionName, l2JobConfig);
        l2WorkflowItem.addWorkflowStatusListener(new ProductSetSaver(l2WorkflowItem, productSet, outputDir));
        return l2WorkflowItem;
    }

    String getResultingProductionType(ProcessorDescriptor processorDescriptor) {
        if (processorDescriptor != null) {
            return processorDescriptor.getOutputProductType();
        }
        return "L2_PRODUCT";
    }
}
