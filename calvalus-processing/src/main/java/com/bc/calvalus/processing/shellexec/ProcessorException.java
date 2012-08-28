package com.bc.calvalus.processing.shellexec;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
@Deprecated
class ProcessorException extends RuntimeException {
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
