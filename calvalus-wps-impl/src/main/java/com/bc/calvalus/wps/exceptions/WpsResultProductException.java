package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class WpsResultProductException extends Exception {

    public WpsResultProductException(String message) {
        super(message);
    }

    public WpsResultProductException(String message, Throwable cause) {
        super(message, cause);
    }

    public WpsResultProductException(Throwable cause) {
        super(cause);
    }
}
