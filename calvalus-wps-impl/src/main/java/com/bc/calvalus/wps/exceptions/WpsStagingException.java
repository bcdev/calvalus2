package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class WpsStagingException extends Exception {

    public WpsStagingException(String message) {
        super(message);
    }

    public WpsStagingException(String message, Throwable cause) {
        super(message, cause);
    }

    public WpsStagingException(Throwable cause) {
        super(cause);
    }
}
