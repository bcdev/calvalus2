package com.bc.calvalus.experiments.executables;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ProcessorException extends RuntimeException {
    public ProcessorException() {
        super();
    }

    public ProcessorException(String message) {
        super(message);
    }

    public ProcessorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProcessorException(Throwable cause) {
        super(cause);
    }
}
