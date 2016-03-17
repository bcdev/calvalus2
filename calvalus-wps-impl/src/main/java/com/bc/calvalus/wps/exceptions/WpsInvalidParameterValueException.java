package com.bc.calvalus.wps.exceptions;

import com.bc.wps.api.WpsRuntimeException;

/**
 * @author hans
 */
public class WpsInvalidParameterValueException extends WpsRuntimeException {

    public WpsInvalidParameterValueException(String invalidValue) {
        super("Invalid value of parameter '" + invalidValue + "'");
    }

}
