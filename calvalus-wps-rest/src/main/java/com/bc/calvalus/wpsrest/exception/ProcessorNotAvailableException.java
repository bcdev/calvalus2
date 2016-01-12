package com.bc.calvalus.wpsrest.exception;

/**
 * @author hans
 */
public class ProcessorNotAvailableException extends WpsRuntimeException {

    public ProcessorNotAvailableException(String processorId) {
        super("Invalid processorId '" + processorId + "'");
    }

    public ProcessorNotAvailableException(String processorId, Throwable cause) {
        super("Invalid processorId '" + processorId + "'", cause);
    }

    public ProcessorNotAvailableException(Throwable cause) {
        super(cause);
    }

}
