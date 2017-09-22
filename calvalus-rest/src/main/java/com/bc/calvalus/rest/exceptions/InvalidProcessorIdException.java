package com.bc.calvalus.rest.exceptions;

/**
 * @author hans
 */
public class InvalidProcessorIdException extends Exception {

    public InvalidProcessorIdException(String processorId) {
        super("Invalid processorId '" + processorId + "'");
    }

}
