package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.*;

abstract class L3ProcessingRequest {
    private final ProductionRequest productionRequest;
    private static long outputFileNum = 0;

    protected L3ProcessingRequest(ProductionRequest productionRequest) {
        this.productionRequest = productionRequest;
    }

    public Map<String, Object> getProcessingParameters() throws ProductionException {
        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        checkSet(productionParameters, "l2ProcessorBundleName");
        checkSet(productionParameters, "l2ProcessorBundleVersion");
        checkSet(productionParameters, "l2ProcessorName");
        checkSet(productionParameters, "l2ProcessorParameters");
        checkSet(productionParameters, "superSampling");
        checkSet(productionParameters, "maskExpr");
        checkSet(productionParameters, "outputStaging");

        HashMap<String, Object> processingParameters = new HashMap<String, Object>(productionParameters);

        processingParameters.put("inputFiles", getInputFiles());
        processingParameters.put("outputDir", getOutputDir());
        processingParameters.put("numRows", getNumRows());
        processingParameters.put("bbox", getBBox());
        processingParameters.put("variables", getVariables());
        processingParameters.put("aggregators", getAggregators());
        processingParameters.put("outputStaging", getOutputStaging());

        return processingParameters;
    }

    public String getOutputFormat() throws ProductionException {
       return getProductionParameterSafe("outputFormat");
    }

    public BeamL3Config.AggregatorConfiguration[] getAggregators() throws ProductionException {
        String inputVariablesStr = getProductionParameterSafe("inputVariables");
        String aggregator = getProductionParameterSafe("aggregator");
        double weightCoeff = Double.parseDouble(getProductionParameterSafe("weightCoeff"));

        String[] inputVariables = inputVariablesStr.split(",");
        BeamL3Config.AggregatorConfiguration[] aggregatorConfigurations = new BeamL3Config.AggregatorConfiguration[inputVariables.length];
        for (int i = 0; i < inputVariables.length; i++) {
            aggregatorConfigurations[i] = new BeamL3Config.AggregatorConfiguration(aggregator,
                                                                                   inputVariables[i],
                                                                                   weightCoeff);
        }
        return aggregatorConfigurations;
    }

    public BeamL3Config.VariableConfiguration[] getVariables() throws ProductionException {
        // todo - implement
        return new BeamL3Config.VariableConfiguration[0];
    }

    public boolean getOutputStaging() throws ProductionException {
        return Boolean.parseBoolean(getProductionParameterSafe("outputStaging"));
    }

    public String getBBox() throws ProductionException {
        return String.format("%s,%s,%s,%s",
                             getProductionParameterSafe("lonMin"), getProductionParameterSafe("latMin"),
                             getProductionParameterSafe("lonMax"), getProductionParameterSafe("latMax"));
    }

    public int getNumRows() throws ProductionException {
        double resolution = Double.parseDouble(getProductionParameterSafe("resolution"));
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

    public abstract String[] getInputFiles() throws ProductionException;

    public String getOutputDir() {
        String outputFileName = productionRequest.getProductionParameters().get("outputFileName");
        if (outputFileName == null) {
            outputFileName = "output-${user}-${num}";
        }
        return outputFileName
                .replace("${user}", System.getProperty("user.name", "hadoop"))
                .replace("${type}", productionRequest.getProductionType())
                .replace("${num}", (++outputFileNum) + "");
    }

    String getProductionParameterSafe(String name) throws ProductionException {
        String value = productionRequest.getProductionParameter(name);
        if (value == null) {
            throw new ProductionException("Missing production parameter '" + name + "'");
        }
        return value;
    }

    void checkSet(Map<String, String> transformed, String name) throws ProductionException {
        if (transformed.get(name) == null) {
            throw new ProductionException("Missing production parameter '" + name + "'");
        }
    }
}
