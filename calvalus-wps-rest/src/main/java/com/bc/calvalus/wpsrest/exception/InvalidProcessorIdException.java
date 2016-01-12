package com.bc.calvalus.wpsrest.exception;

/**
 * @author hans
 */
public class InvalidProcessorIdException extends Exception {

    public InvalidProcessorIdException(String processorId) {
        super("Invalid processorId '" + processorId + "'");
    }

    public InvalidProcessorIdException(String processorId, Throwable cause) {
        super("Invalid processorId '" + processorId + "'", cause);
    }

    public InvalidProcessorIdException(Throwable cause) {
        super(cause);
    }

}
