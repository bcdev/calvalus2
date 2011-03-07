package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.production.ProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.*;

class L3ProcessingRequestFactory extends ProcessingRequestFactory {

    L3ProcessingRequestFactory(ProcessingService processingService, String localStagingDir) {
        super(processingService, localStagingDir);
    }

    @Override
    public L3ProcessingRequest[] createProcessingRequests(String productionId, String userName, ProductionRequest productionRequest) throws ProductionException {

        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        productionRequest.ensureProductionParameterSet("l2ProcessorBundleName");
        productionRequest.ensureProductionParameterSet("l2ProcessorBundleVersion");
        productionRequest.ensureProductionParameterSet("l2ProcessorName");
        productionRequest.ensureProductionParameterSet("l2ProcessorParameters");
        productionRequest.ensureProductionParameterSet("superSampling");
        productionRequest.ensureProductionParameterSet("maskExpr");

        Map<String, Object> commonProcessingParameters = new HashMap<String, Object>(productionParameters);
        commonProcessingParameters.put("outputDir", getOutputDir(productionId, userName, productionRequest));
        commonProcessingParameters.put("stagingDir", getStagingDir(productionId, userName, productionRequest));
        commonProcessingParameters.put("numRows", getNumRows(productionRequest));
        commonProcessingParameters.put("bbox", getBBox(productionRequest));
        commonProcessingParameters.put("fillValue", getFillValue(productionRequest));
        commonProcessingParameters.put("weightCoeff", getWeightCoeff(productionRequest));
        commonProcessingParameters.put("variables", getVariables(productionRequest));
        commonProcessingParameters.put("aggregators", getAggregators(productionRequest));
        commonProcessingParameters.put("outputStaging", getOutputStaging(productionRequest));

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
            processingParameters.put("inputFiles", getInputFiles(productionRequest, date1, date2));
            time += periodLengthMillis;

            processingRequests[i] = new L3ProcessingRequest(processingParameters);
        }

        return processingRequests;
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

    String[] getInputFiles(ProductionRequest request, Date startDate, Date stopDate) throws ProductionException {
         String eoDataPath = getProcessingService().getDataArchiveRootPath();
         String inputProductSetId = request.getProductionParameterSafe("inputProductSetId");
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
