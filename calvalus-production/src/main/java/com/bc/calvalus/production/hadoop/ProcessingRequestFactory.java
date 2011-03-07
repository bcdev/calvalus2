package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.esa.beam.framework.datamodel.ProductData;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Generates processing requests from production requests.
 *
 * @author Norman
 */
abstract class ProcessingRequestFactory {
    private final static DateFormat dateFormat = ProductData.UTC.createDateFormat("yyyy-MM-dd");
    static int outputFileNum;

    private final ProcessingService processingService;
    private final String localStagingDir;

    ProcessingRequestFactory(ProcessingService processingService, String localStagingDir) {
        this.processingService = processingService;
        this.localStagingDir = localStagingDir;
    }

    public String getLocalStagingDir() {
        return localStagingDir;
    }

    public ProcessingService getProcessingService() {
        return processingService;
    }

    public abstract ProcessingRequest[] createProcessingRequests(String productionId, String userName, ProductionRequest productionRequest) throws ProductionException;

    public String getOutputFileName(String productionId, String userName, ProductionRequest request) throws ProductionException {
        String outputFileName = request.getProductionParameter("outputFileName");
        if (outputFileName == null) {
            outputFileName = "${user}-${id}/output-${num}";
        }
        return outputFileName
                .replace("${id}", productionId)
                .replace("${user}", userName)
                .replace("${type}", request.getProductionType())
                .replace("${num}", (++outputFileNum) + "");   // todo - outputFileNum not unique
    }

    public String getOutputDir(String productionId, String userName, ProductionRequest request) throws ProductionException {
        return processingService.getDataOutputRootPath() + "/"
                + getOutputFileName(productionId, userName, request);
    }

    public String getStagingDir(String productionId, String userName, ProductionRequest request) throws ProductionException {
        return localStagingDir + "/"
                + getOutputFileName(productionId, userName, request);
    }

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


    protected Double getDouble(ProductionRequest request, String name, Double def) {
        String fillValueStr = request.getProductionParameter(name);
        if (fillValueStr != null) {
            return Double.parseDouble(fillValueStr);
        } else {
            return def;
        }
    }

    protected Date getDate(ProductionRequest productionRequest, String name) throws ProductionException {
        try {
            return dateFormat.parse(productionRequest.getProductionParameterSafe(name));
        } catch (ParseException e) {
            throw new ProductionException("Illegal date format for production parameter '" + name + "'");
        }
    }

    public static DateFormat getDateFormat() {
        return dateFormat;
    }
}
