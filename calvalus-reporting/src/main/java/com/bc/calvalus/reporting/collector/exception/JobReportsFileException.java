package com.bc.calvalus.reporting.collector.exception;

/**
 * @author hans
 */
public class JobReportsFileException extends Exception {

    public JobReportsFileException(String exceptionMessage) {
        super(exceptionMessage);
    }

    public JobReportsFileException(String exceptionMessage, Throwable throwable) {
        super(exceptionMessage, throwable);
    }
}
