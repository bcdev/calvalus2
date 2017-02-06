package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class WpsProcessorNotFoundException extends Exception {

    public WpsProcessorNotFoundException(String message) {
        super(message);
    }

    public WpsProcessorNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public WpsProcessorNotFoundException(Throwable cause) {
        super(cause);
    }
}
