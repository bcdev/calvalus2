package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.boostrapping.BootstrappingWorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Boostrapping.
 *
 * @author MarcoZ
 * @author MarcoP
 */
public class BootstrappingProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new BootstrappingProductionType(fileSystemService, processing, staging);
        }
    }

    BootstrappingProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                                StagingService stagingService) {
        super("Bootstrapping", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Bootstrapping ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        String outputDir = getOutputPath(productionRequest, productionId, "");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

        WorkflowItem workflowItem = new BootstrappingWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                                  productionName, jobConfig);

        return new Production(productionId,
                              productionName,
                              outputDir,
                              productionRequest.getStagingDirectory(productionId),
                              productionRequest.isAutoStaging(),
                              productionRequest,
                              workflowItem);
    }

}
