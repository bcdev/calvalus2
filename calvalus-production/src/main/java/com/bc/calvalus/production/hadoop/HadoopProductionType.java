package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
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
 * Abstract base class for production types that require a Hadoop processing system.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class HadoopProductionType implements ProductionType {
    private final static DateFormat dateFormat = ProductData.UTC.createDateFormat("yyyy-MM-dd");
    private final String name;
    private final HadoopProcessingService processingService;
    private final StagingService stagingService;

    protected HadoopProductionType(String name, HadoopProcessingService processingService, StagingService stagingService) {
        this.name = name;
        this.processingService = processingService;
        this.stagingService = stagingService;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return getName().equalsIgnoreCase(productionRequest.getProductionType());
    }

    @Override
    public Staging createStaging(Production production) throws ProductionException {
        Staging staging = createUnsubmittedStaging(production);
        try {
            getStagingService().submitStaging(staging);
        } catch (IOException e) {
            throw new ProductionException(String.format("Failed to order staging for production '%s': %s",
                                                        production.getId(), e.getMessage()), e);
        }
        return staging;
    }

    protected abstract Staging createUnsubmittedStaging(Production production);

    public static List<String> getDayPathList(Date start, Date stop, String prefix) {
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

    public HadoopProcessingService getProcessingService() {
        return processingService;
    }

    public StagingService getStagingService() {
        return stagingService;
    }

    public String[] getInputFiles(String inputProductSetId, Date startDate, Date stopDate) throws ProductionException {
        String eoDataPath = processingService.getDataInputPath();
        List<String> dayPathList = getDayPathList(startDate, stopDate, inputProductSetId);
        try {
            List<String> inputFileList = new ArrayList<String>();
            for (String dayPath : dayPathList) {
                String[] strings = processingService.listFilePaths(eoDataPath + "/" + dayPath);
                inputFileList.addAll(Arrays.asList(strings));
            }
            return inputFileList.toArray(new String[inputFileList.size()]);
        } catch (IOException e) {
            throw new ProductionException("Failed to compute input file list.", e);
        }
    }

    public static boolean isAutoStaging(ProductionRequest request) throws ProductionException {
        return getBoolean(request, "autoStaging", false);
    }

    // todo - move to ProductionRequest
    public static String getBBox(ProductionRequest request) throws ProductionException {
        return String.format("%s,%s,%s,%s",
                             request.getProductionParameterSafe("lonMin"),
                             request.getProductionParameterSafe("latMin"),
                             request.getProductionParameterSafe("lonMax"),
                             request.getProductionParameterSafe("latMax"));
    }

    // todo - move to ProductionRequest
    public static boolean getBoolean(ProductionRequest request, String name, Boolean def) {
        String text = request.getProductionParameter(name);
        if (text != null) {
            return Boolean.parseBoolean(text);
        } else {
            return def;
        }
    }

    // todo - move to ProductionRequest
    public static Double getDouble(ProductionRequest request, String name, Double def) {
        String text = request.getProductionParameter(name);
        if (text != null) {
            return Double.parseDouble(text);
        } else {
            return def;
        }
    }

    // todo - move to ProductionRequest
    public static Integer getInteger(ProductionRequest request, String name, Integer def) {
        String text = request.getProductionParameter(name);
        if (text != null) {
            return Integer.parseInt(text);
        } else {
            return def;
        }
    }

    // todo - move to ProductionRequest
    public static Date getDate(ProductionRequest productionRequest, String name) throws ProductionException {
        try {
            return dateFormat.parse(productionRequest.getProductionParameterSafe(name));
        } catch (ParseException e) {
            throw new ProductionException("Illegal date format for production parameter '" + name + "'");
        }
    }

   // todo - move to ProductionRequest
     public static DateFormat getDateFormat() {
        return dateFormat;
    }
}
