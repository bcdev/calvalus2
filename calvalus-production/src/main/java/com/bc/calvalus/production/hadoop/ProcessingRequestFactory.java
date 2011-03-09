package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.StagingService;
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
    private final StagingService stagingService;

    ProcessingRequestFactory(ProcessingService processingService, StagingService stagingService) {
        this.processingService = processingService;
        this.stagingService = stagingService;
    }

    public ProcessingService getProcessingService() {
        return processingService;
    }

    public StagingService getStagingService() {
        return stagingService;
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

    public String getStagingDir(String productionId, String userName, ProductionRequest request) throws ProductionException {
        return stagingService.getStagingAreaPath() + "/"
                + getOutputFileName(productionId, userName, request);
    }

    public boolean isAutoStaging(ProductionRequest request) throws ProductionException {
        return Boolean.parseBoolean(request.getProductionParameterSafe("autoStaging"));
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
