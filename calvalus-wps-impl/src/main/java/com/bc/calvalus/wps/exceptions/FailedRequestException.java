package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class FailedRequestException extends Exception {

    public FailedRequestException(String message) {
        super(message);
    }

    public FailedRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public FailedRequestException(Throwable cause) {
        super(cause);
    }

}
