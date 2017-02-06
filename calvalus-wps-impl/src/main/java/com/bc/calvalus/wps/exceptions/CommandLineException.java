package com.bc.calvalus.wps.exceptions;

import java.io.IOException;

/**
 * @author hans
 */
public class CommandLineException extends IOException {

    public CommandLineException(String exceptionMessage) {
        super(exceptionMessage);
    }

    public CommandLineException(String exceptionMessage, Throwable throwable) {
        super(exceptionMessage, throwable);
    }
}
