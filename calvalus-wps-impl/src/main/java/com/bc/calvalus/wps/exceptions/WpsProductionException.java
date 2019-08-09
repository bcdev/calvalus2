package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class WpsProductionException extends Exception {

    public WpsProductionException(String message) {
        super(message);
    }

    public WpsProductionException(String message, Throwable cause) {
        super(message, cause);
    }

    public WpsProductionException(Throwable cause) {
        super(cause);
    }
}
