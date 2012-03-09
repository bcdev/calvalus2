package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.analysis.QLWorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.production.DateRange;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.operator.AggregatorConfig;
import org.esa.beam.binning.operator.VariableConfig;
import org.esa.beam.binning.support.SEAGrid;
import org.esa.beam.util.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L3ProductionType extends HadoopProductionType {

    public static final String NAME = "L3";

    static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

    public L3ProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = L2ProductionType.createProductionName("Level 3 ", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);

        String inputPath = productionRequest.getString("inputPath");
        List<DateRange> dateRanges = getDateRanges(productionRequest, 10);

        String processorName = productionRequest.getString("processorName", null);
        String processorParameters = null;
        String processorBundle = null;
        if (processorName != null) {
            processorParameters = productionRequest.getString("processorParameters", "<parameters/>");
            processorBundle = String.format("%s-%s",
                                            productionRequest.getString("processorBundleName"),
                                            productionRequest.getString("processorBundleVersion"));
        }

        String regionName = productionRequest.getRegionName();
        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        String l3ConfigXml = getL3ConfigXml(productionRequest);
        String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null));
        String outputDir = getOutputPath(productionRequest, productionId, "-L3-output");

        Workflow workflow = new Workflow.Parallel();
        workflow.setSustainable(false);
        for (int i = 0; i < dateRanges.size(); i++) {
            DateRange dateRange = dateRanges.get(i);
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            String[] l1InputFiles = getInputPaths(getInventoryService(), inputPath, dateRange.getStartDate(), dateRange.getStopDate(), regionName);
            if (l1InputFiles.length > 0) {
                String singleRangeOutputDir = getOutputPath(productionRequest, productionId, "-L3-" + (i + 1));
                Configuration jobConfig = createJobConfig(productionRequest);
                jobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(l1InputFiles, ","));
                jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, singleRangeOutputDir);
                if (processorName != null) {
                    jobConfig.set(JobConfigNames.CALVALUS_L2_BUNDLE, processorBundle);
                    jobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, processorName);
                    jobConfig.set(JobConfigNames.CALVALUS_L2_PARAMETERS, processorParameters);
                }
                jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
                jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry != null ? regionGeometry.toString() : "");
                jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
                jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
                WorkflowItem item = new L3WorkflowItem(getProcessingService(), productionName + " " + date1Str, jobConfig);

                if (outputFormat != null) {
                    jobConfig = createJobConfig(productionRequest);
                    jobConfig.set(JobConfigNames.CALVALUS_INPUT, singleRangeOutputDir);
                    jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
                    jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

                    // is in fact dependent on the outputFormat TODO unify
                    String outputCompression = productionRequest.getString("outputCompression", productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz"));
                    jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, outputCompression);

                    jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
                    jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry != null ? regionGeometry.toString() : "");
                    jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
                    jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

                    WorkflowItem formatItem = new L3FormatWorkflowItem(getProcessingService(), productionName + " Format " + date1Str, jobConfig);
                    item = new Workflow.Sequential(item, formatItem);
                }
                workflow.add(item);
            }
        }
        if (workflow.getItems().length == 0) {
            throw new ProductionException("No input products found for given time range.");
        }
        if (productionRequest.getString(JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS, null) != null) {
            Configuration qlJobConfig = createJobConfig(productionRequest);
            qlJobConfig.set(JobConfigNames.CALVALUS_INPUT, outputDir);
            qlJobConfig.set(JobConfigNames.CALVALUS_INPUT_FORMAT, outputFormat);
            qlJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

            WorkflowItem qlItem = new QLWorkflowItem(getProcessingService(), productionName + " RGB", qlJobConfig);
            workflow = new Workflow.Sequential(workflow, qlItem);
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              null, // no dedicated output directory
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflow);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new CopyStaging(production,
                               getProcessingService().getJobClient().getConf(),
                               getStagingService().getStagingDir());
    }

    static List<DateRange> getDateRanges(ProductionRequest productionRequest, int periodLengthDefault) throws ProductionException {
        List<DateRange> dateRangeList = new ArrayList<DateRange>();
        Date[] dateList = productionRequest.getDates("dateList", null);

        if (dateList != null) {
            for (Date date : dateList) {
                dateRangeList.add(new DateRange(date, date));
            }
        } else {
            Date minDate = productionRequest.getDate("minDate");
            Date maxDate = productionRequest.getDate("maxDate");
            int periodLength = productionRequest.getInteger("periodLength", periodLengthDefault); // unit=days
            int compositingPeriodLength = productionRequest.getInteger("compositingPeriodLength", periodLength); // unit=days

            final long periodLengthMillis = periodLength * MILLIS_PER_DAY;
            final long compositingPeriodLengthMillis = compositingPeriodLength * MILLIS_PER_DAY;

            long time = minDate.getTime();
            while (true) {

                // we subtract 1 ms for date2, because when formatting to date format we would get the following day.
                Date date1 = new Date(time);
                Date date2 = new Date(time + compositingPeriodLengthMillis - 1L);

                if (date2.after(new Date(maxDate.getTime() + MILLIS_PER_DAY))) {
                    break;
                }
                dateRangeList.add(new DateRange(date1, date2));
                time += periodLengthMillis;
            }
        }

        return dateRangeList;
    }

    public static String getL3ConfigXml(ProductionRequest productionRequest) throws ProductionException {
        String l3ConfigXml = productionRequest.getString(JobConfigNames.CALVALUS_L3_PARAMETERS, null);
        if (l3ConfigXml == null) {
            L3Config l3Config = getL3Config(productionRequest);
            l3ConfigXml = l3Config.toXml();
        } else {
            // Check L3 XML before sending it to Hadoop
            try {
                L3Config.fromXml(l3ConfigXml);
            } catch (BindingException e) {
                throw new ProductionException("Illegal L3 configuration: " + e.getMessage(), e);
            }
        }
        return l3ConfigXml;
    }

    static L3Config getL3Config(ProductionRequest productionRequest) throws ProductionException {
        L3Config l3Config = new L3Config();
        l3Config.setNumRows(getNumRows(productionRequest));
        l3Config.setSuperSampling(productionRequest.getInteger("superSampling", 1));
        l3Config.setMaskExpr(productionRequest.getString("maskExpr", ""));
        l3Config.setVariableConfigs(getVariables(productionRequest));
        l3Config.setAggregatorConfigs(getAggregators(productionRequest));
        return l3Config;
    }

    static AggregatorConfig[] getAggregators(ProductionRequest request) throws ProductionException {
        int variableCount = request.getInteger("variables.count");
        AggregatorConfig[] aggregatorConfigs = new AggregatorConfig[variableCount];
        for (int i = 0; i < variableCount; i++) {
            String prefix = "variables." + i;
            String variableName = request.getString(prefix + ".name");
            String aggregatorName = request.getString(prefix + ".aggregator");
            Double weightCoeff = request.getDouble(prefix + ".weightCoeff", null);
            Integer percentage = request.getInteger(prefix + ".percentage", null); //unused in portal
            Float fillValue = request.getFloat(prefix + ".fillValue", null); //unused in portal

            AggregatorConfig aggregatorConfig = new AggregatorConfig(aggregatorName);
            aggregatorConfig.setVarName(variableName);
            aggregatorConfig.setPercentage(percentage);
            aggregatorConfig.setWeightCoeff(weightCoeff);
            aggregatorConfig.setFillValue(fillValue);
            aggregatorConfigs[i] = aggregatorConfig;
        }
        return aggregatorConfigs;
    }

    static VariableConfig[] getVariables(ProductionRequest request) throws ProductionException {
        int expressionCount = request.getInteger("expression.count", 0);
        VariableConfig[] variableConfigs = new VariableConfig[expressionCount];
        for (int i = 0; i < expressionCount; i++) {
            String prefix = "expression." + i;
            String name = request.getString(prefix + ".variable");
            String exp = request.getString(prefix + ".expression");
            variableConfigs[i] = new VariableConfig(name, exp);
        }
        return variableConfigs;
    }

    static int getNumRows(ProductionRequest request) throws ProductionException {
        double resolution = request.getDouble("resolution");
        return SEAGrid.computeRowCount(resolution);
    }

}
