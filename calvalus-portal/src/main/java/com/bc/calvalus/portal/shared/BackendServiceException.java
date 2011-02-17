package com.bc.calvalus.portal.shared;

public class BackendServiceException extends Exception {
    BackendServiceException() {
    }

    public BackendServiceException(String message) {
        super(message);
    }

    public BackendServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
