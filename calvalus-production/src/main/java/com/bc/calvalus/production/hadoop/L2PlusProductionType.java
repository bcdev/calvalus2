package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.l2.L2FormattingWorkflowItem;
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
public class L2PlusProductionType extends HadoopProductionType {

    static final String NAME = "L2Plus";

    public L2PlusProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                                StagingService stagingService) {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProdcutionName(
                createProductionName("Level 2 ", productionRequest));

        List<DateRange> dateRanges = productionRequest.getDateRanges();

        HadoopWorkflowItem processingItem = createProcessingItem(productionId, productionName, dateRanges,
                                                                 productionRequest);
        WorkflowItem workflowItem = processingItem;
        String outputDir = processingItem.getOutputDir();

        String outputFormat = productionRequest.getString("outputFormat", null);
        if (outputFormat != null && !outputFormat.equals("SEQ")) {
            String formattingInputDir = outputDir;
            HadoopWorkflowItem formattingItem = createFormattingItem(productionId, productionName, dateRanges,
                                                                     formattingInputDir, productionRequest);
            outputDir = formattingItem.getOutputDir();
            workflowItem = new Workflow.Sequential(processingItem, formattingItem);
        }

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDir,
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


    HadoopWorkflowItem createFormattingItem(String productionId,
                                            String productionName,
                                            List<DateRange> dateRanges,
                                            String formattingInputDir,
                                            ProductionRequest productionRequest) throws ProductionException {

        Configuration formatJobConfig = createJobConfig(productionRequest);

        String pathPattern = createPathPattern(formattingInputDir);
        formatJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, pathPattern);
        formatJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        String formatOutputDir = getOutputPath(productionRequest, productionId, "-output");
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, formatOutputDir);
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, productionRequest.getString("outputFormat"));

        Geometry regionGeom = productionRequest.getRegionGeometry(null);
        formatJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                            regionGeom != null ? regionGeom.toString() : "");
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_CRS, productionRequest.getString("outputCRS"));
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_BANDLIST, productionRequest.getString("outputBandList"));

        return new L2FormattingWorkflowItem(getProcessingService(), productionName, formatJobConfig);
    }

    private HadoopWorkflowItem createProcessingItem(String productionId,
                                                    String productionName,
                                                    List<DateRange> dateRanges,
                                                    ProductionRequest productionRequest) throws ProductionException {

        productionRequest.ensureParameterSet("processorName");

        String outputDir = getOutputPath(productionRequest, productionId, "");

        Configuration l2JobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(l2JobConfig, processorProductionRequest);
        setRequestParameters(l2JobConfig, productionRequest);


        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        Geometry regionGeom = productionRequest.getRegionGeometry(null);
        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeom != null ? regionGeom.toString() : "");

        Date startDate = dateRanges.get(0).getStartDate();
        Date stopDate = dateRanges.get(dateRanges.size() - 1).getStopDate();
        String pathPattern = createPathPattern(outputDir);
        String regionWKT = regionGeom != null ? regionGeom.toString() : null;
        ProcessorDescriptor processorDescriptor = processorProductionRequest.getProcessorDescriptor(
                getProcessingService());
        ProductSet productSet = new ProductSet(getResultingProductionType(processorDescriptor),
                                               productionName, pathPattern, startDate, stopDate,
                                               productionRequest.getRegionName(), regionWKT);

        HadoopWorkflowItem l2Item = new L2WorkflowItem(getProcessingService(), productionName, l2JobConfig);
        l2Item.addWorkflowStatusListener(new ProductSetSaver(l2Item, productSet, outputDir));
        return l2Item;
    }

    // TODO consider l2Gen output here too; this is only valid for sequential and MERIS files
    private String createPathPattern(String basePath) {
        return basePath + "/.*${yyyy}${MM}${dd}.*.seq$";
    }

    private String getResultingProductionType(ProcessorDescriptor processorDescriptor) {
        if (processorDescriptor != null) {
            return processorDescriptor.getOutputProductType();
        }
        return "L2_PRODUCT";
    }

}
