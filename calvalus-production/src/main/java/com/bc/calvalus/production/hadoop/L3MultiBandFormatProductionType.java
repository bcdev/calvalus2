package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.multiband.L3MultiBandFormatWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;

/**
 * A production type used for formatting Level-3 products into one (GeoTIFF) file per band
 *
 * @author Martin Böttcher, BC, 2014
 */
public class L3MultiBandFormatProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new L3MultiBandFormatProductionType(inventory, processing, staging);
        }
    }

    L3MultiBandFormatProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                                    StagingService stagingService) {
        super("L3FBands", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        final String defaultProductionName = createProductionName("Level-3 Multi-Band Formatting", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        final String inputPath = productionRequest.getString("inputPath");
        final String outputPath = getOutputPath(productionRequest, productionId, "");
        final String outputFormat = productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "GeoTIFF");
        final String outputBandList = productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_BANDLIST);
        final String stagingDir = productionRequest.getStagingDirectory(productionId);
        final boolean autoStaging = productionRequest.isAutoStaging();
        final ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);

        final Configuration jobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, inputPath);
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_BANDLIST, outputBandList);

        WorkflowItem workflowItem = new L3MultiBandFormatWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                                      productionName, jobConfig);

        return new Production(productionId,
                              productionName,
                              outputPath,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging L3MultiBand not yet implemented");
    }
}
