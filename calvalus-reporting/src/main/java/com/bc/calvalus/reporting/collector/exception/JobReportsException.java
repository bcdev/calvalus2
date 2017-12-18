package com.bc.calvalus.reporting.collector.exception;

/**
 * @author hans
 */
public class JobReportsException extends Exception {

    public JobReportsException(String exceptionMessage) {
        super(exceptionMessage);
    }

    public JobReportsException(String exceptionMessage, Throwable throwable) {
        super(exceptionMessage, throwable);
    }
}
