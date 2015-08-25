package com.bc.calvalus.wpsrest;

/**
 * Created by hans on 14/08/2015.
 */
public class WpsException extends Exception {

    public WpsException(String message) {
        super(message);
    }

    public WpsException(String message, Throwable cause) {
        super(message, cause);
    }

    public WpsException(Throwable cause) {
        super(cause);
    }

}
