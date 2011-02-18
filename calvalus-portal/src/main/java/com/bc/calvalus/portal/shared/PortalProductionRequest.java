package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

public class PortalProductionRequest implements IsSerializable {
    String inputProductSetId;
    String outputProductSetName;
    String processorId;
    String processorVersion;
    String processingParameters;

    /**
     * No-arg constructor as required by {@link IsSerializable}.
     */
    public PortalProductionRequest() {
    }

    public PortalProductionRequest(String inputProductSetId,
                                   String outputProductSetName,
                                   String processorId,
                                   String processorVersion,
                                   String processingParameters) {
        this.inputProductSetId = inputProductSetId;
        this.outputProductSetName = outputProductSetName;
        this.processorId = processorId;
        this.processorVersion = processorVersion;
        this.processingParameters = processingParameters;
    }

    public static boolean isValid(PortalProductionRequest req) {
        if (req.getInputProductSetId() == null || req.getInputProductSetId().isEmpty()) {
            return false;
        }
        if (req.getOutputProductSetName() == null || req.getOutputProductSetName().isEmpty()) {
            return false;
        }
        if (req.getProcessorId() == null || req.getProcessorId().isEmpty()) {
            return false;
        }
        return true;
    }

    public String getInputProductSetId() {
        return inputProductSetId;
    }

    public String getOutputProductSetName() {
        return outputProductSetName;
    }

    public String getProcessorId() {
        return processorId;
    }

    public String getProcessorVersion() {
        return processorVersion;
    }

    public String getProcessingParameters() {
        return processingParameters;
    }
}
