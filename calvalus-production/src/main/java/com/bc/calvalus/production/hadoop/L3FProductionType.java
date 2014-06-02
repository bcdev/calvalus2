package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.analysis.QLWorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * A production type used for formatting one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L3FProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new L3FProductionType(inventory, processing, staging);
        }
    }

    L3FProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                      StagingService stagingService) {
        super("L3F", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Level-3 Format", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        setRequestParameters(productionRequest, jobConfig);

        jobConfig.setStrings(JobConfigNames.CALVALUS_INPUT_DIR, productionRequest.getString("inputPath"));

        String outputDir = getOutputPath(productionRequest, productionId, "");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

        String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(
                        JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

        // is in fact dependent on the outputFormat TODO unify
        String outputCompression = productionRequest.getString("outputCompression", productionRequest.getString(
                JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, outputCompression);

        WorkflowItem workflow = new L3FormatWorkflowItem(getProcessingService(),
                                                         productionRequest.getUserName(),
                                                         productionName + " Format", jobConfig);


        if (productionRequest.getString(JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS, null) != null) {
            Configuration qlJobConfig = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, qlJobConfig);

            qlJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, outputDir + "/[^_].*");
            qlJobConfig.set(JobConfigNames.CALVALUS_INPUT_FORMAT, outputFormat);
            qlJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

            WorkflowItem qlItem = new QLWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                     productionName + " RGB", qlJobConfig);
            workflow = new Workflow.Sequential(workflow, qlItem);
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflow);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        return new CopyStaging(production,
                               getProcessingService().getJobClient(production.getProductionRequest().getUserName()).getConf(),
                               getStagingService().getStagingDir());
    }
}
