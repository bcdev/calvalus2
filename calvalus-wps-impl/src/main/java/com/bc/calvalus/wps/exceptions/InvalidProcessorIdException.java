package com.bc.calvalus.wps.exceptions;

/**
 * @author hans
 */
public class InvalidProcessorIdException extends Exception {

    public InvalidProcessorIdException(String processorId) {
        super("Invalid processorId '" + processorId + "'");
    }

}
