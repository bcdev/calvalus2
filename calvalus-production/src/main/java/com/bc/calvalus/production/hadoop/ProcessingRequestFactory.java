package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Generates processing requests from production requests.
 *
 * @author Norman
 */
abstract class ProcessingRequestFactory {
    private final static DateFormat dateFormat = ProductData.UTC.createDateFormat("yyyy-MM-dd");

    private final ProcessingService processingService;

    ProcessingRequestFactory(ProcessingService processingService) {
        this.processingService = processingService;
    }

    public ProcessingService getProcessingService() {
        return processingService;
    }

    public abstract ProcessingRequest[] createProcessingRequests(String productionId, String userName, ProductionRequest productionRequest) throws ProductionException;


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
        String text = request.getProductionParameter(name);
        if (text != null) {
            return Double.parseDouble(text);
        } else {
            return def;
        }
    }

    protected Integer getInteger(ProductionRequest request, String name, Integer def) {
        String text = request.getProductionParameter(name);
        if (text != null) {
            return Integer.parseInt(text);
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

    String[] getInputFiles(String inputProductSetId, Date startDate, Date stopDate) throws ProductionException {
        String eoDataPath = getProcessingService().getDataInputPath();
        List<String> dayPathList = getDayPathList(startDate, stopDate, inputProductSetId);
        try {
            List<String> inputFileList = new ArrayList<String>();
            for (String dayPath : dayPathList) {
                String[] strings = getProcessingService().listFilePaths(eoDataPath + "/" + dayPath);
                inputFileList.addAll(Arrays.asList(strings));
            }
            return inputFileList.toArray(new String[inputFileList.size()]);
        } catch (IOException e) {
            throw new ProductionException("Failed to compute input file list.", e);
        }
    }

    static List<String> getDayPathList(Date start, Date stop, String prefix) {
        Calendar startCal = ProductData.UTC.createCalendar();
        Calendar stopCal = ProductData.UTC.createCalendar();
        startCal.setTime(start);
        stopCal.setTime(stop);
        List<String> list = new ArrayList<String>();
        do {
            String dateString = String.format("MER_RR__1P/r03/%1$tY/%1$tm/%1$td", startCal);
            if (dateString.startsWith(prefix)) {
                list.add(dateString);
            }
            startCal.add(Calendar.DAY_OF_WEEK, 1);
        } while (!startCal.after(stopCal));

        return list;
    }

}
