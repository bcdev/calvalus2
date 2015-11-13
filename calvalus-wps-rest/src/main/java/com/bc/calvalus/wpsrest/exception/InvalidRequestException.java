package com.bc.calvalus.wpsrest.exception;

/**
 * Created by hans on 14/08/2015.
 */
public class InvalidRequestException extends WpsException {

    public InvalidRequestException(String message) {
        super(message);
    }

    public InvalidRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidRequestException(Throwable cause) {
        super(cause);
    }

}
