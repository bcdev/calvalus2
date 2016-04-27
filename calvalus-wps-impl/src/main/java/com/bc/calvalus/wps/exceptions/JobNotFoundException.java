package com.bc.calvalus.wps.exceptions;

import com.bc.wps.api.exceptions.InvalidParameterValueException;

/**
 * @author hans
 */
public class JobNotFoundException extends InvalidParameterValueException {

    public JobNotFoundException(Throwable cause, String invalidParameter) {
        super(cause, invalidParameter);
    }

    public JobNotFoundException(String invalidParameter) {
        super(invalidParameter);
    }

}
