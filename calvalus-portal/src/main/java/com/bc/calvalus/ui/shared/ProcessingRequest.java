package com.bc.calvalus.ui.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

public class ProcessingRequest implements IsSerializable {
    String inputProductSet;
    String outputProductSet;
    String processorName;
    String processingParameters;

    public ProcessingRequest() {
    }

    public ProcessingRequest(String inputProductSet, String outputProductSet, String processorName, String processingParameters) {
        this.inputProductSet = inputProductSet;
        this.outputProductSet = outputProductSet;
        this.processorName = processorName;
        this.processingParameters = processingParameters;
    }

    public static boolean isValid(ProcessingRequest req) {
        if (req.getInputProductSet() == null || req.getInputProductSet().isEmpty()) {
            return false;
        }
        if (req.getProcessorName() == null || req.getProcessorName().isEmpty()) {
            return false;
        }
        return true;
    }

    public String getInputProductSet() {
        return inputProductSet;
    }

    public void setInputProductSet(String inputProductSet) {
        this.inputProductSet = inputProductSet;
    }

    public String getOutputProductSet() {
        return outputProductSet;
    }

    public void setOutputProductSet(String outputProductSet) {
        this.outputProductSet = outputProductSet;
    }

    public String getProcessorName() {
        return processorName;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    public String getProcessingParameters() {
        return processingParameters;
    }

    public void setProcessingParameters(String processingParameters) {
        this.processingParameters = processingParameters;
    }
}
