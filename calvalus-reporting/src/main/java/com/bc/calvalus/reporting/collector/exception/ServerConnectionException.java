package com.bc.calvalus.reporting.collector.exception;

/**
 * @author hans
 */
public class ServerConnectionException extends Exception {

    public ServerConnectionException(String exceptionMessage) {
        super(exceptionMessage);
    }

    public ServerConnectionException(Throwable throwable) {
        super(throwable);
    }

    public ServerConnectionException(String exceptionMessage, Throwable throwable) {
        super(exceptionMessage, throwable);
    }
}
