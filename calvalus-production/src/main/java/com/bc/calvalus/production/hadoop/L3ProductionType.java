package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.Math.PI;

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
        final String productionName = createL3ProductionName(productionRequest);
        final String userName = productionRequest.getUserName();

        String inputPath = productionRequest.getString("inputPath");
        List<DatePair> datePairList = getDatePairList(productionRequest, 10);

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

        Workflow.Parallel workflow = new Workflow.Parallel();
        for (int i = 0; i < datePairList.size(); i++) {
            DatePair datePair = datePairList.get(i);
            String date1Str = ProductionRequest.getDateFormat().format(datePair.date1);
            String date2Str = ProductionRequest.getDateFormat().format(datePair.date2);
            String[] l1InputFiles = getInputPaths(inputPath, datePair.date1, datePair.date2, regionName);
            if (l1InputFiles.length > 0) {
                String outputDir = getOutputPath(productionRequest, productionId, "-L3-" + (i + 1));
                Configuration jobConfig = createJobConfig(productionRequest);
                jobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(l1InputFiles, ","));
                jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
                jobConfig.set(JobConfigNames.CALVALUS_L2_BUNDLE, processorBundle);
                jobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, processorName);
                jobConfig.set(JobConfigNames.CALVALUS_L2_PARAMETERS, processorParameters);
                jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
                jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry != null ? regionGeometry.toString() : "");
                jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
                jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
                L3WorkflowItem l3WorkflowItem = new L3WorkflowItem(getProcessingService(),
                                                                   productionId + "_" + (i + 1), jobConfig);
                workflow.add(l3WorkflowItem);
            }
        }
        if (workflow.getItems().length == 0) {
            throw new ProductionException("No input products found for given time range.");
        }

        String stagingDir = userName + "/" + productionId;
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflow);
    }

    public static int computeDefaultPeriodLength(Date minDate, Date maxDate, int periodCount) {
        return (int) ((maxDate.getTime() - minDate.getTime() + MILLIS_PER_DAY - 1) / (MILLIS_PER_DAY * periodCount));
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new L3Staging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    static String createL3ProductionName(ProductionRequest productionRequest) throws ProductionException {
        return String.format("Level 3 production using input path '%s' and L2 processor '%s'",
                             productionRequest.getString("inputPath"),
                             productionRequest.getString("processorName"));

    }

    static List<DatePair> getDatePairList(ProductionRequest productionRequest, int periodLengthDefault) throws ProductionException {
        List<DatePair> datePairList = new ArrayList<DatePair>();
        Date[] dateList = productionRequest.getDates("dateList", null);

        if (dateList != null) {
            for (Date date : dateList) {
                datePairList.add(new DatePair(date, date));
            }
        } else {
            Date minDate = productionRequest.getDate("minDate");
            Date maxDate = productionRequest.getDate("maxDate");
            int periodLength = productionRequest.getInteger("periodLength", periodLengthDefault); // unit=days
            int compositingPeriodLength = productionRequest.getInteger("compositingPeriodLength", periodLength); // unit=days

            final long periodLengthMillis = periodLength * MILLIS_PER_DAY;
            final long compositingPeriodLengthMillis = compositingPeriodLength * MILLIS_PER_DAY;

            long time = minDate.getTime();
            for (int i = 0; ; i++) {

                // we subtract 1 ms for date2, because when formatting to date format we would get the following day.
                Date date1 = new Date(time);
                Date date2 = new Date(time + compositingPeriodLengthMillis - 1L);

                if (date2.after(new Date(maxDate.getTime() + MILLIS_PER_DAY))) {
                    break;
                }
                datePairList.add(new DatePair(date1, date2));
                time += periodLengthMillis;
            }
        }

        return datePairList;
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
        l3Config.setVariables(getVariables(productionRequest));
        l3Config.setAggregators(getAggregators(productionRequest));
        return l3Config;
    }

    static L3Config.AggregatorConfiguration[] getAggregators(ProductionRequest request) throws ProductionException {
        int variableCount = request.getInteger("variables.count");
        L3Config.AggregatorConfiguration[] aggregatorConfigurations = new L3Config.AggregatorConfiguration[variableCount];
        for (int i = 0; i < variableCount; i++) {
            String prefix = "variables." + i;
            String variableName = request.getString(prefix + ".name");
            String aggregatorName = request.getString(prefix + ".aggregator");
            Double weightCoeff = request.getDouble(prefix + ".weightCoeff", null);
            Integer percentage = request.getInteger(prefix + ".percentage", null); //unused in portal
            Float fillValue = request.getFloat(prefix + ".fillValue", null); //unused in portal

            L3Config.AggregatorConfiguration aggregatorConfiguration = new L3Config.AggregatorConfiguration(aggregatorName);
            aggregatorConfiguration.setVarName(variableName);
            aggregatorConfiguration.setPercentage(percentage);
            aggregatorConfiguration.setWeightCoeff(weightCoeff);
            aggregatorConfiguration.setFillValue(fillValue);
            aggregatorConfigurations[i] = aggregatorConfiguration;
        }
        return aggregatorConfigurations;
    }

    static L3Config.VariableConfiguration[] getVariables(ProductionRequest request) throws ProductionException {
        // todo - implement L3 variables
        return new L3Config.VariableConfiguration[0];
    }

    static int getNumRows(ProductionRequest request) throws ProductionException {
        double resolution = request.getDouble("resolution");
        return computeBinningGridRowCount(resolution);
    }

    static int computeBinningGridRowCount(double res) {
        // see: SeaWiFS Technical Report Series Vol. 32;
        final double RE = 6378.145;
        int numRows = 1 + (int) Math.floor(0.5 * (2 * PI * RE) / res);
        if (numRows % 2 == 0) {
            return numRows;
        } else {
            return numRows + 1;
        }
    }

    static class DatePair {
        final Date date1;
        final Date date2;

        DatePair(Date date1, Date date2) {
            this.date1 = date1;
            this.date2 = date2;
        }
    }
}
