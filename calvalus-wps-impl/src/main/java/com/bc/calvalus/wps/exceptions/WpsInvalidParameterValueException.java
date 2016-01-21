package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class WpsInvalidParameterValueException extends WpsRuntimeException {

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
