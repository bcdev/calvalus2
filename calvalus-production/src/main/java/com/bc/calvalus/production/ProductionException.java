package com.bc.calvalus.production;

/**
* An exception that may be thrown by the {@link ProductionService}.
*
* @author Norman
*/
public class ProductionException extends Exception {

    public ProductionException(String message) {
        super(message);
    }

    public ProductionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProductionException(Throwable cause) {
        super(cause);
    }
}
