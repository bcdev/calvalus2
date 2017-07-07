package com.bc.calvalus.reporting.collector.exception;

/**
 * @author hans
 */
public class JobTransformerException extends Exception {

    public JobTransformerException(String exceptionMessage) {
        super(exceptionMessage);
    }

    public JobTransformerException(Throwable throwable) {
        super(throwable);
    }

    public JobTransformerException(String exceptionMessage, Throwable throwable) {
        super(exceptionMessage, throwable);
    }
}
