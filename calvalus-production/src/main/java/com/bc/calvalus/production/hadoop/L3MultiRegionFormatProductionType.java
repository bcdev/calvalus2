package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionFormatWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * A production type used for formatting Level-3 products for
 * multiple regions
 *
 * @author MarcoZ
 */
public class L3MultiRegionFormatProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new L3MultiRegionFormatProductionType(inventory, processing, staging);
        }
    }

    L3MultiRegionFormatProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                                      StagingService stagingService) {
        super("L3MultiRegion", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Level-3 Multi-Region-Formatting", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        Configuration jobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        String outputDir= getOutputPath(productionRequest, productionId, "");

        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, productionRequest.getString("inputPath"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

        String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(
                JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

        // is in fact dependent on the outputFormat TODO unify
        String outputCompression = productionRequest.getString("outputCompression", productionRequest.getString(
                JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, outputCompression);


        WorkflowItem workflowItem = new L3MultiRegionFormatWorkflowItem(getProcessingService(), productionName, jobConfig);

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
        throw new NotImplementedException("Staging currently not implemented for L3MultiRegion.");
    }
}
