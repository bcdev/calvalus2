package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.FileSystemService;
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
import com.bc.ceres.binding.BindingException;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Trend analysis: A production type to generate a time-series of L3 products for a number of
 * regions.
 *
 * @author MarcoZ
 * @author Norman
 * @author Martin
 */
public class TAProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new TAProductionType(fileSystemService, processing, staging);
        }
    }

    TAProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super("TA", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        // extract request parameters
        final List<DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, "1m");
        if (dateRanges.size() == 0) {
            throw new ProductionException("Time range is zero");
        }
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProductionName(createProductionName("Trend analysis ", productionRequest));
        final Geometry regionGeometry = productionRequest.getRegionGeometry();
        final String l3ConfigXml = L3ProductionType.getL3ConfigXml(productionRequest);
        TAConfig taConfig = null;
        if (productionRequest.getParameters().containsKey(JobConfigNames.CALVALUS_TA_PARAMETERS)) {
            final String taParameters = productionRequest.getParameter(JobConfigNames.CALVALUS_TA_PARAMETERS, true);
            try {
                taConfig = TAConfig.fromXml(taParameters);
            } catch (BindingException e) {
                throw new ProductionException("Parsing request ta parameters failed: " + taParameters);
            }
        }
        if (taConfig == null || taConfig.getRegions().length == 0) {
            taConfig = new TAConfig(new TAConfig.RegionConfiguration(productionRequest.getString("regionName"), regionGeometry));
        }
        final String stagingDir = productionRequest.getStagingDirectory(productionId);
        final boolean autoStaging = productionRequest.getBoolean("autoStaging", true);
        final boolean isSkipL3 = productionRequest.getBoolean(JobConfigNames.CALVALUS_TA_SKIPL3_FLAG, false);

        // construct workflow
        Workflow.Sequential taSequence = new Workflow.Sequential();
        Workflow.Parallel l3Set = new Workflow.Parallel();
        StringBuffer l3OutputDirs = new StringBuffer();

        // add L3 items
        for (int i = 0; i < dateRanges.size(); i++) {
            final DateRange dateRange = dateRanges.get(i);
            final String centreDate = ProductionRequest.getDateFormat().format(new Date((dateRange.getStartDate().getTime() + dateRange.getStopDate().getTime()) / 2));
            final String l3OutputDir = getOutputPath(productionRequest, productionId, "-L3-" + centreDate);
            if (! isSkipL3) {
                final L3WorkflowItem l3WorkflowItem = createL3WorkflowItem(productionRequest, productionName, l3ConfigXml, regionGeometry, dateRange, centreDate, l3OutputDir);
                l3Set.add(l3WorkflowItem);
            }
            if (i != 0) {
                l3OutputDirs.append(",");
            }
            l3OutputDirs.append(l3OutputDir);
        }

        // add TA item
        final String taOutputDir = getOutputPath(productionRequest, productionId, "-TA");
        final TAWorkflowItem taWorkflowItem = createTaWorkflowItem(productionRequest, productionName, l3ConfigXml, taConfig, dateRanges, l3OutputDirs, taOutputDir);
        if (! isSkipL3) {
            taSequence.add(l3Set);
        }
        taSequence.add(taWorkflowItem);

        // return production of workflow
        return new Production(productionId,
                              productionName,
                              taOutputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              taSequence);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        return new TAStaging(production,
                             getProcessingService().getJobClient(production.getProductionRequest().getUserName()).getConf(),
                             getStagingService().getStagingDir());
    }

    private L3WorkflowItem createL3WorkflowItem(ProductionRequest productionRequest, String productionName, String l3ConfigXml, Geometry regionGeometry, DateRange dateRange, String centreDate, String l3OutputDir) throws ProductionException {
        final String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
        final String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
        final String l3JobName = String.format("%s L3 %s", productionName, centreDate);
        final ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);

        final Configuration l3JobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, l3JobConfig);
        setRequestParameters(productionRequest, l3JobConfig);
        processorProductionRequest.configureProcessor(l3JobConfig);
        setInputLocationParameters(productionRequest, l3JobConfig);
        l3JobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        l3JobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dateRange.toString());
        l3JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, l3OutputDir);
        l3JobConfig.unset(JobConfigNames.CALVALUS_OUTPUT_FORMAT);
        l3JobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
        l3JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry != null ? regionGeometry.toString() : "");
        l3JobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
        l3JobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

        return new L3WorkflowItem(getProcessingService(), productionRequest.getUserName(), l3JobName, l3JobConfig);
    }

    private TAWorkflowItem createTaWorkflowItem(ProductionRequest productionRequest, String productionName, String l3ConfigXml, TAConfig taConfig, List<DateRange> dateRanges, StringBuffer l3OutputDirs, String taOutputDir) throws ProductionException {
        final String date1Str = ProductionRequest.getDateFormat().format(dateRanges.get(0).getStartDate());
        final String date2Str = ProductionRequest.getDateFormat().format(dateRanges.get(dateRanges.size() - 1).getStopDate());
        final String taJobName = String.format("%s TA", productionName);
        final boolean withTimeseriesPlot = productionRequest.getBoolean(JobConfigNames.TA_WITH_TIMESERIES_PLOT, true);
        final boolean withAggregatedCsv = productionRequest.getBoolean(JobConfigNames.TA_WITH_AGGREGATED_CSV, true);
        final boolean withPixelCsv = productionRequest.getBoolean(JobConfigNames.TA_WITH_PIXEL_CSV, true);
        final boolean withL3Outputs = productionRequest.getBoolean(JobConfigNames.TA_WITH_L3_OUTPUTS, false);

        final Configuration taJobConfig = createJobConfig(productionRequest);
        setRequestParameters(productionRequest, taJobConfig);
        taJobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, l3OutputDirs.toString());
        taJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, taOutputDir);
        taJobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
        taJobConfig.set(JobConfigNames.CALVALUS_TA_PARAMETERS, taConfig.toXml());
        taJobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
        taJobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
        taJobConfig.set("numRegions", String.valueOf(taConfig.getRegions().length));
        taJobConfig.set("mapred.reduce.tasks", String.valueOf(Math.min(taConfig.getRegions().length, 64)));

        return new TAWorkflowItem(getProcessingService(), productionRequest.getUserName(), taJobName, taJobConfig);
    }
}
