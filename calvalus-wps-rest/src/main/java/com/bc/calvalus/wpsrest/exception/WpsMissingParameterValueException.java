package com.bc.calvalus.wpsrest.exception;

/**
 * @author hans
 */
public class WpsMissingParameterValueException extends WpsRuntimeException {

    public WpsMissingParameterValueException(String missingParameter) {
        super("Missing value from parameter : " + missingParameter);
    }

    public WpsMissingParameterValueException(String missingParameter, Throwable cause) {
        super("Missing value from parameter : " + missingParameter, cause);
    }

    public WpsMissingParameterValueException(Throwable cause) {
        super(cause);
    }
}
