package com.bc.calvalus.wps.exceptions;

import com.bc.wps.api.WpsRuntimeException;

/**
 * @author hans
 */
public class WpsMissingParameterValueException extends WpsRuntimeException {

    public WpsMissingParameterValueException(String missingParameter) {
        super("Missing value from parameter : " + missingParameter);
    }

}
