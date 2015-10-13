package com.bc.calvalus.wpsrest.exception;

/**
 * Created by hans on 08/10/2015.
 */
public class WpsInvalidParameterValueException extends WpsException {

    public WpsInvalidParameterValueException(String invalidValue) {
        super("Invalid value of parameter '" + invalidValue + "'");
    }

    public WpsInvalidParameterValueException(String invalidValue, Throwable cause) {
        super("Invalid value of parameter '" + invalidValue + "'", cause);
    }

    public WpsInvalidParameterValueException(Throwable cause) {
        super(cause);
    }
}
