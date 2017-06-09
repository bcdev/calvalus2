package com.bc.calvalus.reporting.restservice.exceptions;

/**
 * @author hans
 */
public class ExtractionException extends Exception {

    public ExtractionException(Throwable cause) {
        super(cause);
    }

    public ExtractionException(Throwable cause, String invalidParameter) {
        super(invalidParameter, cause);
    }

    public ExtractionException(String invalidParameter) {
        super(invalidParameter);
    }

}
