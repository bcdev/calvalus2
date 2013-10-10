package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.processing.ta.TAConfig;
import com.bc.calvalus.processing.ta.TAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Trend analysis: A production type used for generating a time-series generated from L3 products and a numb er of
 * given regions.
 *
 * @author MarcoZ
 * @author Norman
 */
public class TAProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new TAProductionType(inventory, processing, staging);
        }
    }

    TAProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super("TA", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Trend analysis ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        List<DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, 32);

        Geometry regionGeometry = productionRequest.getRegionGeometry();

        String l3ConfigXml = L3ProductionType.getL3ConfigXml(productionRequest);
        TAConfig taConfig = getTAConfig(productionRequest);

        Workflow.Parallel parallel = new Workflow.Parallel();
        for (int i = 0; i < dateRanges.size(); i++) {
            DateRange dateRange = dateRanges.get(i);
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());

            Workflow.Sequential sequential = new Workflow.Sequential();

            String l3JobName = String.format("%s L3-%s", productionName, date1Str);
            String taJobName = String.format("%s TA-%s", productionName, date1Str);

            String l3OutputDir = getOutputPath(productionRequest, productionId, "-L3-" + (i + 1));
            String taOutputDir = getOutputPath(productionRequest, productionId, "-TA-" + (i + 1));

            Configuration l3JobConfig = createJobConfig(productionRequest);
            ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
            setDefaultProcessorParameters(processorProductionRequest, l3JobConfig);
            setRequestParameters(productionRequest, l3JobConfig);
            processorProductionRequest.configureProcessor(l3JobConfig);

            l3JobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
            l3JobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            l3JobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dateRange.toString());

            l3JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, l3OutputDir);
            l3JobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
            l3JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                            regionGeometry != null ? regionGeometry.toString() : "");
            l3JobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            l3JobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
            L3WorkflowItem l3WorkflowItem = new L3WorkflowItem(getProcessingService(), l3JobName, l3JobConfig);

            Configuration taJobConfig = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, taJobConfig);
            taJobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, l3OutputDir);
            taJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, taOutputDir);
            taJobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
            taJobConfig.set(JobConfigNames.CALVALUS_TA_PARAMETERS, taConfig.toXml());
            taJobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            taJobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
            TAWorkflowItem taWorkflowItem = new TAWorkflowItem(getProcessingService(), taJobName, taJobConfig);

            sequential.add(l3WorkflowItem);
            sequential.add(taWorkflowItem);

            parallel.add(sequential);
        }
        if (parallel.getItems().length == 0) {
            throw new ProductionException("No input products found for given time range.");
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.getBoolean("autoStaging", true);
        return new Production(productionId,
                              productionName,
                              null, // no dedicated output directory
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              parallel);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new TAStaging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    static TAConfig getTAConfig(ProductionRequest productionRequest) throws ProductionException {
        String regionName = productionRequest.getString("regionName");
        Geometry regionGeometry = productionRequest.getRegionGeometry();
        TAConfig.RegionConfiguration regionConfiguration = new TAConfig.RegionConfiguration(regionName, regionGeometry);
        return new TAConfig(regionConfiguration);
    }
}
