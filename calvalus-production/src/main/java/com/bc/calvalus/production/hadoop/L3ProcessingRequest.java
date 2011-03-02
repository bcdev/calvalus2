package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.util.HashMap;
import java.util.Map;

abstract class L3ProcessingRequest {
    private final ProductionRequest productionRequest;
    private static long outputFileNum = 0;

    protected L3ProcessingRequest(ProductionRequest productionRequest) {
        this.productionRequest = productionRequest;
    }

    public ProductionRequest getProductionRequest() {
        return productionRequest;
    }

    public Map<String, Object> getProcessingParameters() throws ProductionException {
        HashMap<String, Object> context = new HashMap<String, Object>(productionRequest.getProductionParameters());

        checkSet(context, "productionId");
        checkSet(context, "productionName");
        checkSet(context, "l2OperatorName");
        checkSet(context, "l2OperatorParameters");
        checkSet(context, "superSampling");
        checkSet(context, "validMask");
        checkSet(context, "processorPackage");
        checkSet(context, "processorVersion");
        context.put("inputFiles", getInputFiles());
        context.put("outputDir", getOutputFileName());
        context.put("numRows", getNumRows());
        context.put("bbox", getBBox());
        context.put("variables", getVariables());
        context.put("aggregators", getAggregators());

        //context.put("processorPackage", "beam-lkn");
        //context.put("processorVersion", "1.0-SNAPSHOT");

        return context;
    }

    public BeamL3Config.AggregatorConfiguration[] getAggregators() throws ProductionException {
        String inputVariablesStr = getProductionParameter("inputVariables");
        String aggregator = getProductionParameter("aggregator");
        double weightCoeff = Double.parseDouble(getProductionParameter("weightCoeff"));

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

    public String getBBox() throws ProductionException {
        return String.format("%s,%s,%s,%s",
                             getProductionParameter("lonMin"), getProductionParameter("latMin"),
                             getProductionParameter("lonMax"), getProductionParameter("latMax"));
    }

    public int getNumRows() throws ProductionException {
        double resolution = Double.parseDouble(getProductionParameter("resolution"));
        return HadoopProductionService.getNumRows(resolution);
    }

    public abstract String[] getInputFiles() throws ProductionException;

    public String getOutputFileName() {
        String outputFileName = productionRequest.getProductionParameters().get("outputFileName");
        if (outputFileName == null) {
            outputFileName = "output-${user}-${num}";
        }
        return outputFileName
                .replace("${user}", System.getProperty("user.name", "hadoop"))
                .replace("${type}", productionRequest.getProductionType())
                .replace("${num}", (++outputFileNum) + "");
    }

    String getProductionParameter(String name) throws ProductionException {
        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        String value = productionParameters.get(name);
        if (value == null) {
            throw new ProductionException("Missing production parameter '" + name + "'");
        }
        return value;
    }

    void checkSet(Map<String, ?> transformed, String name) throws ProductionException {
        if (transformed.get(name) == null) {
            throw new ProductionException("Missing production parameter '" + name + "'");
        }
    }
}
