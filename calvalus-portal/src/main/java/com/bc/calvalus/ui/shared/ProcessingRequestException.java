package com.bc.calvalus.ui.shared;

public class ProcessingRequestException extends Exception {
    ProcessingRequestException() {
    }

    public ProcessingRequestException(String message) {
        super(message);
    }

    public ProcessingRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
