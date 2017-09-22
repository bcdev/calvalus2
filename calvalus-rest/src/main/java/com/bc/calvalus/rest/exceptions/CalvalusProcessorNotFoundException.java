package com.bc.calvalus.rest.exceptions;

/**
 * @author hans
 */
public class CalvalusProcessorNotFoundException extends Exception {

    public CalvalusProcessorNotFoundException(String message) {
        super(message);
    }

    public CalvalusProcessorNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public CalvalusProcessorNotFoundException(Throwable cause) {
        super(cause);
    }
}

