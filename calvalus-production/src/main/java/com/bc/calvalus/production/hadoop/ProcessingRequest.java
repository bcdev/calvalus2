package com.bc.calvalus.production.hadoop;

import java.util.Map;

class ProcessingRequest {
    private final Map<String, Object> processingParameters;

    public ProcessingRequest(Map<String, Object> processingParameters) {
        this.processingParameters = processingParameters;
    }

    public Map<String, Object> getProcessingParameters() {
        return processingParameters;
    }

    public String getOutputFormat() {
        return getProcessingParameter("outputFormat");
    }

    public Boolean getOutputStaging() {
        return getProcessingParameter("outputStaging");
    }

    public String getBBox() {
        return getProcessingParameter("bbox");
    }

    public String[] getInputFiles() {
        return getProcessingParameter("inputFiles");
    }

    public String getOutputDir() {
        return getProcessingParameter("outputDir");
    }

    public <T> T getProcessingParameter(String name) {
        return (T) processingParameters.get(name);
    }
}
