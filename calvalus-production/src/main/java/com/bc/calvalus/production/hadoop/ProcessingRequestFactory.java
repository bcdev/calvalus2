package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

/**
 * Generates processing requests from production requests.
 *
 * @author Norman
 */
abstract class ProcessingRequestFactory {
    static int outputFileNum;

    public abstract ProcessingRequest createProcessingRequest(ProductionRequest productionRequest) throws ProductionException;

    public abstract String[] getInputFiles(ProductionRequest request) throws ProductionException;

    public boolean getOutputStaging(ProductionRequest request) throws ProductionException {
        return Boolean.parseBoolean(request.getProductionParameterSafe("outputStaging"));
    }

    public String getBBox(ProductionRequest request) throws ProductionException {
        return String.format("%s,%s,%s,%s",
                             request.getProductionParameterSafe("lonMin"),
                             request.getProductionParameterSafe("latMin"),
                             request.getProductionParameterSafe("lonMax"),
                             request.getProductionParameterSafe("latMax"));
    }

    public String getOutputDir(ProductionRequest request) throws ProductionException {
        String outputFileName = request.getProductionParameterSafe("outputFileName");
        if (outputFileName == null) {
            outputFileName = "output-${user}-${num}";
        }
        return outputFileName
                .replace("${user}", System.getProperty("user.name"))
                .replace("${type}", request.getProductionType())
                .replace("${num}", (++outputFileNum) + "");
    }

    protected Double getDouble(ProductionRequest request, String name, Double def) {
        String fillValueStr = request.getProductionParameter(name);
        if (fillValueStr != null) {
            return Double.parseDouble(fillValueStr);
        } else {
            return def;
        }
    }

}
