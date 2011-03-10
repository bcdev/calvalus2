package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.processing.beam.FormatterL3Config;

import java.io.File;
import java.util.Map;

class L3ProcessingRequest extends ProcessingRequest {

    public L3ProcessingRequest(Map<String, Object> processingParameters) {
        super(processingParameters);
    }

    public Double getFillValue() {
        return getProcessingParameter("fillValue");
    }

    public BeamL3Config.AggregatorConfiguration[] getAggregators() {
        return getProcessingParameter("aggregators");
    }

    public BeamL3Config.VariableConfiguration[] getVariables() {
        return getProcessingParameter("variables");
    }

    public Integer getNumRows() {
        return getProcessingParameter("numRows");
    }

    public BeamL3Config getBeamL3Config() {
        BeamL3Config beamL3Config = new BeamL3Config();
        beamL3Config.setNumRows(getNumRows());
        String superSampling = getProcessingParameter("superSampling");
        beamL3Config.setSuperSampling(Integer.parseInt(superSampling));
        beamL3Config.setBbox(getBBox());
        String maskExpr = getProcessingParameter("maskExpr");
        beamL3Config.setMaskExpr(maskExpr);
        beamL3Config.setVariables(getVariables());
        beamL3Config.setAggregators(getAggregators());
        return beamL3Config;
    }

    public FormatterL3Config getFormatterL3Config(String stagingPath) {
        String dateStart = getProcessingParameter("dateStart");
        String dateStop = getProcessingParameter("dateStop");

        String outputFormat = getOutputFormat();
        String extension;

        if (outputFormat.equals("BEAM-DIMAP")) {
            extension = "dim";
        } else if (outputFormat.equals("NetCDF")) {
            extension = "nc";
        } else {
            extension = "xxx"; // todo  what else to handle ?
        }
        String filename = String.format("L3_%s-%s.%s", dateStart, dateStop, extension);  // todo - from processingRequest
        String stagingFilePath = new File(stagingPath, filename).getPath();

        FormatterL3Config.BandConfiguration[] bands = new FormatterL3Config.BandConfiguration[0];  // todo - from processingRequest
        String outputType = "Product"; // todo - from processingRequest

        FormatterL3Config formatConfig = new FormatterL3Config(outputType,
                                                               stagingFilePath,
                                                               outputFormat,
                                                               bands,
                                                               dateStart,
                                                               dateStop);

        return formatConfig;
    }

}
