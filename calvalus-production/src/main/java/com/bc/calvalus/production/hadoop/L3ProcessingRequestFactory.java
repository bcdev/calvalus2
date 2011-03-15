package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.beam.L3Config;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.PI;

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
        productionRequest.ensureProductionParameterSet("superSampling");
        productionRequest.ensureProductionParameterSet("maskExpr");

        Map<String, Object> commonProcessingParameters = new HashMap<String, Object>(productionParameters);
        int numRows = getNumRows(productionRequest);
        commonProcessingParameters.put("numRows", numRows);
        String bBox = getBBox(productionRequest);
        commonProcessingParameters.put("bbox", bBox);
        commonProcessingParameters.put("fillValue", getFillValue(productionRequest));
        commonProcessingParameters.put("weightCoeff", getWeightCoeff(productionRequest));
        L3Config.VariableConfiguration[] variables = getVariables(productionRequest);
        commonProcessingParameters.put("variables", variables);
        L3Config.AggregatorConfiguration[] aggregators = getAggregators(productionRequest);
        commonProcessingParameters.put("aggregators", aggregators);
        String maskExpr = productionRequest.getProductionParameter("maskExpr");
        String superSampling = productionRequest.getProductionParameter("superSampling");

        commonProcessingParameters.put("level3Parameters", getLevel3(numRows, bBox, maskExpr, superSampling, variables, aggregators));

        commonProcessingParameters.put("autoStaging", isAutoStaging(productionRequest));

        int periodCount = Integer.parseInt(productionRequest.getProductionParameter("periodCount"));
        int periodLength = Integer.parseInt(productionRequest.getProductionParameter("periodLength")); // unit=days

        Date startDate = getDate(productionRequest, "dateStart");
        Date stopDate = getDate(productionRequest, "dateStop");
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
            processingParameters.put("outputDir", getProcessingService().getDataOutputPath() +
                    "/" + userName + "/" + productionId + "_" + i);

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

    public Double getFillValue(ProductionRequest request) throws ProductionException {
        return getDouble(request, "fillValue", null);
    }

    public Double getWeightCoeff(ProductionRequest request) throws ProductionException {
        return getDouble(request, "weightCoeff", null);
    }

    public L3Config.AggregatorConfiguration[] getAggregators(ProductionRequest request) throws ProductionException {
        String inputVariablesStr = request.getProductionParameterSafe("inputVariables");
        String aggregator = request.getProductionParameterSafe("aggregator");
        Double weightCoeff = getWeightCoeff(request);
        Double fillValue = getFillValue(request);

        String[] inputVariables = inputVariablesStr.split(",");
        L3Config.AggregatorConfiguration[] aggregatorConfigurations = new L3Config.AggregatorConfiguration[inputVariables.length];
        for (int i = 0; i < inputVariables.length; i++) {
            aggregatorConfigurations[i] = new L3Config.AggregatorConfiguration(aggregator,
                                                                                   inputVariables[i],
                                                                                   weightCoeff,
                                                                                   fillValue);
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
