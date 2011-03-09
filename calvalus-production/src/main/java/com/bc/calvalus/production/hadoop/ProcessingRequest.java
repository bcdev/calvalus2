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

    public String getBBox() {
        return getProcessingParameter("bbox");
    }

    public String[] getInputFiles() {
        return getProcessingParameter("inputFiles");
    }

    public Boolean isAutoStaging() {
        return getProcessingParameter("autoStaging");
    }

    /**
     * The absolute path of the directory used to stage (copy and reformat) the output of a processing job.
     *
     * @return The output directory.
     */
    public String getStagingDir() {
        return getProcessingParameter("stagingDir");
    }

    /**
     * The absolute path of the directory used to store the output of a processing job.
     * @return The output directory.
     */
    public String getOutputDir() {
        return getProcessingParameter("outputDir");
    }

    public <T> T getProcessingParameter(String name) {
        return (T) processingParameters.get(name);
    }
}
