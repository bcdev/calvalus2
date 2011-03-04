package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;

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

}
