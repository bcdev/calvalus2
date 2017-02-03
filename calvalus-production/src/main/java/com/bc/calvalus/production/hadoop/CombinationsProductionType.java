package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.combinations.CombinationsWorkflowItem;
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
 * Combinations.
 *
 * @author MarcoZ
 */
public class CombinationsProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new CombinationsProductionType(fileSystemService, processing, staging);
        }
    }

    CombinationsProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                               StagingService stagingService) {
        super("Combinations", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Combinations ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        String outputDir = getOutputPath(productionRequest, productionId, "");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

        WorkflowItem workflowItem = new CombinationsWorkflowItem(getProcessingService(),
                                                                 productionRequest.getUserName(),
                                                                 productionName,
                                                                 jobConfig);

        return new Production(productionId,
                              productionName,
                              outputDir,
                              productionRequest.getStagingDirectory(productionId),
                              productionRequest.isAutoStaging(),
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        String userName = production.getProductionRequest().getUserName();
        return new CopyStaging(production,
                               getProcessingService().getJobClient(userName).getConf(),
                               getStagingService().getStagingDir());
    }

}
