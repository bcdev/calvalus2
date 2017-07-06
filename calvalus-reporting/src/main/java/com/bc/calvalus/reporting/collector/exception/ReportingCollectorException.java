package com.bc.calvalus.reporting.collector.exception;

/**
 * @author hans
 */
public class ReportingCollectorException extends Exception {

    public ReportingCollectorException(String exceptionMessage) {
        super(exceptionMessage);
    }

    public ReportingCollectorException(String exceptionMessage, Throwable throwable) {
        super(exceptionMessage, throwable);
    }
}
