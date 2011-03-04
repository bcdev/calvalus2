package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.*;

abstract class L3ProcessingRequestFactory extends ProcessingRequestFactory {

    protected L3ProcessingRequestFactory() {
    }

    @Override
    public L3ProcessingRequest createProcessingRequest(ProductionRequest productionRequest) throws ProductionException {

        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        productionRequest.ensureProductionParameterSet("l2ProcessorBundleName");
        productionRequest.ensureProductionParameterSet("l2ProcessorBundleVersion");
        productionRequest.ensureProductionParameterSet("l2ProcessorName");
        productionRequest.ensureProductionParameterSet("l2ProcessorParameters");
        productionRequest.ensureProductionParameterSet("superSampling");
        productionRequest.ensureProductionParameterSet("maskExpr");

        Map<String, Object> processingParameters = new HashMap<String, Object>(productionParameters);
        processingParameters.put("inputFiles", getInputFiles(productionRequest));
        processingParameters.put("outputDir", getOutputDir(productionRequest));
        processingParameters.put("stagingDir", getStagingDir(productionRequest));
        processingParameters.put("numRows", getNumRows(productionRequest));
        processingParameters.put("bbox", getBBox(productionRequest));
        processingParameters.put("fillValue", getFillValue(productionRequest));
        processingParameters.put("weightCoeff", getWeightCoeff(productionRequest));
        processingParameters.put("variables", getVariables(productionRequest));
        processingParameters.put("aggregators", getAggregators(productionRequest));
        processingParameters.put("outputStaging", getOutputStaging(productionRequest));

        return new L3ProcessingRequest(processingParameters);
    }

    public Double getFillValue(ProductionRequest request) throws ProductionException {
        return getDouble(request, "fillValue", null);
    }

    public Double getWeightCoeff(ProductionRequest request) throws ProductionException {
        return getDouble(request, "weightCoeff", null);
    }

    public BeamL3Config.AggregatorConfiguration[] getAggregators(ProductionRequest request) throws ProductionException {
        String inputVariablesStr = request.getProductionParameterSafe("inputVariables");
        String aggregator = request.getProductionParameterSafe("aggregator");
        Double weightCoeff = getWeightCoeff(request);
        Double fillValue = getFillValue(request);

        String[] inputVariables = inputVariablesStr.split(",");
        BeamL3Config.AggregatorConfiguration[] aggregatorConfigurations = new BeamL3Config.AggregatorConfiguration[inputVariables.length];
        for (int i = 0; i < inputVariables.length; i++) {
            aggregatorConfigurations[i] = new BeamL3Config.AggregatorConfiguration(aggregator,
                                                                                   inputVariables[i],
                                                                                   weightCoeff,
                                                                                   fillValue);
        }
        return aggregatorConfigurations;
    }

    public BeamL3Config.VariableConfiguration[] getVariables(ProductionRequest request) throws ProductionException {
        // todo - implement L3 variables
        return new BeamL3Config.VariableConfiguration[0];
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
