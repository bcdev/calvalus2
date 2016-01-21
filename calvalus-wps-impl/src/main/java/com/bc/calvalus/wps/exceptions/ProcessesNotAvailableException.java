package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class ProcessesNotAvailableException extends Exception {

    public ProcessesNotAvailableException(String message) {
        super(message);
    }

    public ProcessesNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProcessesNotAvailableException(Throwable cause) {
        super(cause);
    }

}
