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

    public L3ProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super("calvalus-level3", processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createL3ProductionName(productionRequest);
        final String userName = productionRequest.getUserName();


        String inputProductSetId = productionRequest.getProductionParameterSafe("inputProductSetId");
        Date startDate = productionRequest.getDate("dateStart");
        Date stopDate = productionRequest.getDate("dateStop");  // todo - clarify meaning of this parameter (we use startDate + i * periodLength here)

        String processorName = productionRequest.getProductionParameterSafe("processorName");
        String processorParameters = productionRequest.getProductionParameterSafe("processorParameters");
        String processorBundle = String.format("%s-%s",
                                               productionRequest.getProductionParameterSafe("processorBundleName"),
                                               productionRequest.getProductionParameterSafe("processorBundleVersion"));

        Geometry roiGeometry = productionRequest.getRoiGeometry();

        L3Config l3Config = createBinningConfig(productionRequest);

        int periodCount = Integer.parseInt(productionRequest.getProductionParameter("periodCount"));
        int periodLength = Integer.parseInt(productionRequest.getProductionParameter("periodLength")); // unit=days

        long time = startDate.getTime();
        long periodLengthMillis = periodLength * 24L * 60L * 60L * 1000L;

        Workflow.Parallel workflow = new Workflow.Parallel();
        for (int i = 0; i < periodCount; i++) {

            Date date1 = new Date(time);
            Date date2 = new Date(time + periodLengthMillis - 1L);

            // todo - use geoRegion to filter input files
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
                                                               ProductionRequest.getDateFormat().format(date1),
                                                               ProductionRequest.getDateFormat().format(date2));
            workflow.add(l3WorkflowItem);
            time += periodLengthMillis;
        }

        return new Production(productionId,
                              productionName,
                              userName + "/" + productionId,
                              productionRequest,
                              workflow);
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

    static String createL3ProductionName(ProductionRequest productionRequest) {
        return String.format("Level 3 production using product set '%s' and L2 processor '%s'",
                             productionRequest.getProductionParameter("inputProductSetId"),
                             productionRequest.getProductionParameter("processorName"));

    }

    static L3Config createBinningConfig(ProductionRequest productionRequest) throws ProductionException {
        L3Config l3Config = new L3Config();
        l3Config.setNumRows(getNumRows(productionRequest));
        l3Config.setSuperSampling(Integer.parseInt(productionRequest.getProductionParameter("superSampling")));
        l3Config.setMaskExpr(productionRequest.getProductionParameter("maskExpr"));
        l3Config.setVariables(getVariables(productionRequest));
        l3Config.setAggregators(getAggregators(productionRequest));
        return l3Config;
    }

    static L3Config.AggregatorConfiguration[] getAggregators(ProductionRequest request) throws ProductionException {
        String inputVariablesStr = request.getProductionParameterSafe("inputVariables");
        String aggregatorName = request.getProductionParameterSafe("aggregator");
        Integer percentage = request.getInteger("percentage", null);
        Double weightCoeff = request.getDouble("weightCoeff", null);
        Double fillValue = request.getDouble("fillValue", null);
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
        double resolution = Double.parseDouble(request.getProductionParameterSafe("resolution"));
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
