package com.bc.calvalus.wpsrest.exception;

/**
 * Created by hans on 08/10/2015.
 */
public class WpsMissingParameterValueException extends WpsException {

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
