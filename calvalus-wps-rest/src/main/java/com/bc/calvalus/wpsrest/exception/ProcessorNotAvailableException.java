package com.bc.calvalus.wpsrest.exception;

/**
 * Created by hans on 14/08/2015.
 */
public class ProcessorNotAvailableException extends WpsException {

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
