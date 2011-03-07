package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.File;
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

    public abstract ProcessingRequest[] createProcessingRequests(ProductionRequest productionRequest) throws ProductionException;


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

    public abstract String getStagingDir(ProductionRequest request) throws ProductionException;

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
