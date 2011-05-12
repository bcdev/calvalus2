package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;

import java.util.Date;

import static java.lang.Math.*;

/**
 * A production type used for generating one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L3ProductionType extends HadoopProductionType {

    public static final String NAME = "L3";

    static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

    public L3ProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createL3ProductionName(productionRequest);
        final String userName = productionRequest.getUserName();


        String inputProductSetId = productionRequest.getParameter("inputProductSetId");
        Date minDate = productionRequest.getDate("minDate");
        Date maxDate = productionRequest.getDate("maxDate");

        String processorName = productionRequest.getParameter("processorName");
        String processorParameters = productionRequest.getParameter("processorParameters");
        String processorBundle = String.format("%s-%s",
                                               productionRequest.getParameter("processorBundleName"),
                                               productionRequest.getParameter("processorBundleVersion"));

        Geometry roiGeometry = productionRequest.getRegionGeometry();

        L3Config l3Config = createL3Config(productionRequest);

        int periodLength = productionRequest.getInteger("periodLength", 10); // unit=days
        int compositingPeriodLength = productionRequest.getInteger("compositingPeriodLength", periodLength); // unit=days

        final long periodLengthMillis = periodLength * MILLIS_PER_DAY;
        final long compositingPeriodLengthMillis = compositingPeriodLength * MILLIS_PER_DAY;

        long time = minDate.getTime();
        Workflow.Parallel workflow = new Workflow.Parallel();
        for (int i = 0; ; i++) {

            // we subtract 1 ms for date2, because when formatting to date format we would get the following day.
            Date date1 = new Date(time);
            Date date2 = new Date(time + compositingPeriodLengthMillis - 1L);

            String date1Str = ProductionRequest.getDateFormat().format(date1);
            String date2Str = ProductionRequest.getDateFormat().format(date2);

            if (date2.after(new Date(maxDate.getTime() + MILLIS_PER_DAY))) {
                break;
            }

            // todo - use geoRegion to filter input files (nf,20.04.2011)
            String[] inputFiles = getInputFiles(inputProductSetId, date1, date2);
            String outputDir = getOutputDir(productionRequest.getUserName(), productionId, i+1);

            L3WorkflowItem l3WorkflowItem = new L3WorkflowItem(getProcessingService(),
                                                               productionId + "_" + (i+1),
                                                               processorBundle,
                                                               processorName,
                                                               processorParameters,
                                                               roiGeometry,
                                                               inputFiles,
                                                               outputDir,
                                                               l3Config,
                                                               date1Str,
                                                               date2Str);
            workflow.add(l3WorkflowItem);
            time += periodLengthMillis;
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
        return (int)((maxDate.getTime() - minDate.getTime() + MILLIS_PER_DAY - 1) / (MILLIS_PER_DAY * periodCount));
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new L3Staging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    String getOutputDir(String userName, String productionId, int index) {
        return String.format("%s/%s/%s_%d",
                             getProcessingService().getDataOutputPath(),
                             userName,
                             productionId,
                             index);
    }

    static String createL3ProductionName(ProductionRequest productionRequest) throws ProductionException {
        return String.format("Level 3 production using product set '%s' and L2 processor '%s'",
                             productionRequest.getParameter("inputProductSetId"),
                             productionRequest.getParameter("processorName"));

    }

    static L3Config createL3Config(ProductionRequest productionRequest) throws ProductionException {
        L3Config l3Config = new L3Config();
        l3Config.setNumRows(getNumRows(productionRequest));
        l3Config.setSuperSampling(productionRequest.getInteger("superSampling", 1));
        l3Config.setMaskExpr(productionRequest.getParameter("maskExpr", ""));
        l3Config.setVariables(getVariables(productionRequest));
        l3Config.setAggregators(getAggregators(productionRequest));
        return l3Config;
    }

    static L3Config.AggregatorConfiguration[] getAggregators(ProductionRequest request) throws ProductionException {
        String inputVariablesStr = request.getParameter("inputVariables");
        String aggregatorName = request.getParameter("aggregator");
        Integer percentage = request.getInteger("percentage", null);
        Double weightCoeff = request.getDouble("weightCoeff", null);
        Float fillValue = request.getFloat("fillValue", null);
        String[] inputVariables = inputVariablesStr.split(",");
        for (int i = 0; i < inputVariables.length; i++) {
            inputVariables[i] = inputVariables[i].trim();
        }
        L3Config.AggregatorConfiguration[] aggregatorConfigurations = new L3Config.AggregatorConfiguration[inputVariables.length];
        for (int i = 0; i < inputVariables.length; i++) {
            L3Config.AggregatorConfiguration aggregatorConfiguration = new L3Config.AggregatorConfiguration(aggregatorName);
            aggregatorConfiguration.setVarName(inputVariables[i]);
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
}
