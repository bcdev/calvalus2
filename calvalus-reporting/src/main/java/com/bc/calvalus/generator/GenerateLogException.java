package com.bc.calvalus.generator;

/**
 * @author muhammad.bc.
 */
public class GenerateLogException extends Exception {
    public GenerateLogException(Throwable throwable) {
        super(throwable);
    }

    public GenerateLogException(String msg) {
        super(msg);

    }
}
