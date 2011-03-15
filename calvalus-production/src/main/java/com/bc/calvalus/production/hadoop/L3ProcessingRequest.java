package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.L3Config;
import com.bc.calvalus.processing.beam.L3FormatterConfig;

import java.io.File;
import java.util.Map;

public class L3ProcessingRequest extends ProcessingRequest {

    public L3ProcessingRequest(Map<String, Object> processingParameters) {
        super(processingParameters);
    }

    public Double getFillValue() {
        return getProcessingParameter("fillValue");
    }

    public L3Config.AggregatorConfiguration[] getAggregators() {
        return getProcessingParameter("aggregators");
    }

    public L3Config.VariableConfiguration[] getVariables() {
        return getProcessingParameter("variables");
    }

    public Integer getNumRows() {
        return getProcessingParameter("numRows");
    }

    public L3Config getBeamL3Config() {
        return getProcessingParameter("level3Parameters");
    }

    public L3FormatterConfig getFormatterL3Config(String stagingPath) {
        String dateStart = getProcessingParameter("dateStart");
        String dateStop = getProcessingParameter("dateStop");

        String outputFormat = getOutputFormat();
        String extension;

        if (outputFormat.equals("BEAM-DIMAP")) {
            extension = "dim";
        } else if (outputFormat.equals("NetCDF")) {
            extension = "nc";
        } else if (outputFormat.equals("GeoTIFF")) {
            extension = "tif";
        } else {
            extension = "xxx"; // todo  what else to handle ?
        }
        String filename = String.format("L3_%s-%s.%s", dateStart, dateStop, extension);  // todo - from processingRequest
        String stagingFilePath = new File(stagingPath, filename).getPath();

        L3FormatterConfig.BandConfiguration[] bands = new L3FormatterConfig.BandConfiguration[0];  // todo - from processingRequest
        String outputType = "Product"; // todo - from processingRequest

        L3FormatterConfig formatterConfig = new L3FormatterConfig(outputType,
                                                               stagingFilePath,
                                                               outputFormat,
                                                               bands,
                                                               dateStart,
                                                               dateStop);

        return formatterConfig;
    }

}
