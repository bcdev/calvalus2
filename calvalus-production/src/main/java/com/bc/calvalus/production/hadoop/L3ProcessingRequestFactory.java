package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.beam.L3Config;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.*;

public class L3ProcessingRequestFactory extends ProcessingRequestFactory {

    L3ProcessingRequestFactory(ProcessingService processingService) {
        super(processingService);
    }

    @Override
    public L3ProcessingRequest[] createProcessingRequests(String productionId, String userName, ProductionRequest productionRequest) throws ProductionException {

        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        productionRequest.ensureProductionParameterSet("processorBundleName");
        productionRequest.ensureProductionParameterSet("processorBundleVersion");
        productionRequest.ensureProductionParameterSet("processorName");
        productionRequest.ensureProductionParameterSet("processorParameters");
        productionRequest.ensureProductionParameterSet("maskExpr");
        productionRequest.ensureProductionParameterSet("superSampling");

        Map<String, Object> commonProcessingParameters = new HashMap<String, Object>(productionParameters);

        commonProcessingParameters.put("productionId", productionId);
        commonProcessingParameters.put("userName", userName);
        commonProcessingParameters.put("binningParameters", getLevel3(getNumRows(productionRequest),
                                                                      getBBox(productionRequest),
                                                                      productionRequest.getProductionParameter("maskExpr"),
                                                                      productionRequest.getProductionParameter("superSampling"),
                                                                      getVariables(productionRequest),
                                                                      getAggregators(productionRequest)));
        commonProcessingParameters.put("autoStaging", isAutoStaging(productionRequest));

        int periodCount = Integer.parseInt(productionRequest.getProductionParameter("periodCount"));
        int periodLength = Integer.parseInt(productionRequest.getProductionParameter("periodLength")); // unit=days

        Date startDate = getDate(productionRequest, "dateStart");
        Date stopDate = getDate(productionRequest, "dateStop");  // todo - use stop date
        L3ProcessingRequest[] processingRequests = new L3ProcessingRequest[periodCount];
        long time = startDate.getTime();
        long periodLengthMillis = periodLength * 24L * 60L * 60L * 1000L;

        for (int i = 0; i < periodCount; i++) {

            HashMap<String, Object> processingParameters = new HashMap<String, Object>(commonProcessingParameters);
            Date date1 = new Date(time);
            Date date2 = new Date(time + periodLengthMillis - 1L);
            processingParameters.put("dateStart", getDateFormat().format(date1));
            processingParameters.put("dateStop", getDateFormat().format(date2));
            processingParameters.put("inputFiles", getInputFiles(productionRequest.getProductionParameterSafe("inputProductSetId"), date1, date2));
            processingParameters.put("outputDir", getProcessingService().getDataOutputPath() + "/" + userName + "/" + productionId + "_" + i);

            time += periodLengthMillis;

            processingRequests[i] = new L3ProcessingRequest(processingParameters);
        }

        return processingRequests;
    }

    private L3Config getLevel3(int numRows, String bBox, String maskExpr, String superSampling, L3Config.VariableConfiguration[] variables, L3Config.AggregatorConfiguration[] aggregators) {
        L3Config l3Config = new L3Config();
        l3Config.setNumRows(numRows);
        l3Config.setSuperSampling(Integer.parseInt(superSampling));
        l3Config.setBbox(bBox);
        l3Config.setMaskExpr(maskExpr);
        l3Config.setVariables(variables);
        l3Config.setAggregators(aggregators);
        return l3Config;
    }

    public L3Config.AggregatorConfiguration[] getAggregators(ProductionRequest request) throws ProductionException {
        String inputVariablesStr = request.getProductionParameterSafe("inputVariables");
        String aggregatorName = request.getProductionParameterSafe("aggregator");
        Integer percentage = getInteger(request, "percentage", null);
        Double weightCoeff = getDouble(request, "weightCoeff", null);
        Double fillValue = getDouble(request, "fillValue", null);
        String[] inputVariables = inputVariablesStr.split(",");
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

    public L3Config.VariableConfiguration[] getVariables(ProductionRequest request) throws ProductionException {
        // todo - implement L3 variables
        return new L3Config.VariableConfiguration[0];
    }

    public int getNumRows(ProductionRequest request) throws ProductionException {
        double resolution = Double.parseDouble(request.getProductionParameterSafe("resolution"));
        return computeBinningGridRowCount(resolution);
    }

    public static int computeBinningGridRowCount(double res) {
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
