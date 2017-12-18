package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class SqlStoreException extends Exception {

    public SqlStoreException(String message) {
        super(message);
    }

    public SqlStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlStoreException(Throwable cause) {
        super(cause);
    }
}
