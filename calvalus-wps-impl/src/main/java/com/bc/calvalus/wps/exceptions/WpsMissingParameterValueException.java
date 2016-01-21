package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class WpsMissingParameterValueException extends WpsRuntimeException {

    public WpsMissingParameterValueException(String missingParameter) {
        super("Missing value from parameter : " + missingParameter);
    }

}
