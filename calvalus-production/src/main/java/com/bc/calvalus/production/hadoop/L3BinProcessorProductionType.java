package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3BinProcessorWorkflowItem;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * A production type used for generating Level-3 products starting from L3-Products
 *
 * @author MarcoZ
 */
public class L3BinProcessorProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new L3BinProcessorProductionType(inventory, processing, staging);
        }
    }

    L3BinProcessorProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                                 StagingService stagingService) {
        super("L3Proc", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Level 3 Bin Processing", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        Configuration jobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String regionWKT = regionGeometry != null ? regionGeometry.toString() : "";

        String outputDirParts = getOutputPath(productionRequest, productionId, "-parts");
        String outputDirProducts = getOutputPath(productionRequest, productionId, "-output");
        jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,regionWKT);

        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, productionRequest.getString("inputPath"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDirParts);

        String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(
                JobConfigNames.CALVALUS_OUTPUT_FORMAT, null));

        WorkflowItem workflowItem = new L3BinProcessorWorkflowItem(getProcessingService(), productionName, jobConfig);
        if (outputFormat != null) {
            jobConfig = createJobConfig(productionRequest);
            setDefaultProcessorParameters(processorProductionRequest, jobConfig);
            setRequestParameters(productionRequest, jobConfig);
            processorProductionRequest.configureProcessor(jobConfig);

            jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, outputDirParts);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDirProducts);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

            // is in fact dependent on the outputFormat TODO unify
            String outputCompression = productionRequest.getString("outputCompression", productionRequest.getString(
                    JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz"));
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, outputCompression);

            String l3ConfigXml = L3ProductionType.getL3ConfigXml(productionRequest);
            jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
            jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,regionWKT);

            List<DateRange> dateRanges = productionRequest.getDateRanges();
            DateRange dateRange = dateRanges.get(0);
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

            WorkflowItem formatItem = new L3FormatWorkflowItem(getProcessingService(),
                                                               productionName + " Format " + date1Str, jobConfig);
            workflowItem = new Workflow.Sequential(workflowItem, formatItem);
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDirProducts,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for L3Proc.");
    }
}
