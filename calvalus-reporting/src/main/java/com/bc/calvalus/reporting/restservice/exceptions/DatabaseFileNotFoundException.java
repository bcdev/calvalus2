package com.bc.calvalus.reporting.restservice.exceptions;

/**
 * @author hans
 */
public class DatabaseFileNotFoundException extends RuntimeException {

    public DatabaseFileNotFoundException(String message) {
        super(message);
    }
}
