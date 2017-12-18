package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.CellL3ProcessorWorkflowItem;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * A production type used for generating Level-3 products starting from L3-Products
 *
 * @author MarcoZ
 */
public class CellL3ProcessorProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new CellL3ProcessorProductionType(fileSystemService, processing, staging);
        }
    }

    CellL3ProcessorProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                                  StagingService stagingService) {
        super("CellL3", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Cell Level-3 Processing", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        Configuration jobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        List<DateRange> dateRanges = productionRequest.getDateRanges();
        DateRange dateRange = dateRanges.get(0);
        String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
        String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
        jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
        jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

        String outputDirParts = getOutputPath(productionRequest, productionId, "-parts");
        String outputDirProducts = getOutputPath(productionRequest, productionId, "-output");

        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, productionRequest.getString("inputPath"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDirParts);

        String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(
                JobConfigNames.CALVALUS_OUTPUT_FORMAT, null));

        WorkflowItem workflowItem = new CellL3ProcessorWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                                    productionName, jobConfig);
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

            WorkflowItem formatItem = new L3FormatWorkflowItem(getProcessingService(),
                                                               productionRequest.getUserName(),
                                                               productionName + " Format",
                                                               jobConfig);
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
        throw new UnsupportedOperationException("Staging currently not implemented for L3Proc.");
    }
}
