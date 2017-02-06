package com.bc.calvalus.wps.cmd;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.util.Arrays;
import java.util.List;

/**
 * @author hans
 */
public class CommandLineIO {

    private final ByteArrayOutputStream outputStream;
    private final ByteArrayOutputStream errorStream;
    private CommandLineResultHandler resultHandler;

    public CommandLineIO(ByteArrayOutputStream outputStream, ByteArrayOutputStream errorStream) {
        this.outputStream = outputStream;
        this.errorStream = errorStream;
    }

    public CommandLineIO(ByteArrayOutputStream outputStream, ByteArrayOutputStream errorStream, CommandLineResultHandler resultHandler) {
        this.outputStream = outputStream;
        this.errorStream = errorStream;
        this.resultHandler = resultHandler;
    }

    public List<String> getOutputStringList() {
        String outputString = outputStream.toString();
        String[] outputStringArray = outputString.split("\n");
        return Arrays.asList(outputStringArray);
    }

    public List<String> getErrorStringList() {
        String errorString = errorStream.toString();
        String[] errorStringArray = errorString.split("\n");
        return Arrays.asList(errorStringArray);
    }

    public ByteArrayOutputStream getOutputStream() {
        return outputStream;
    }

    public ByteArrayOutputStream getErrorStream() {
        return errorStream;
    }

    public CommandLineResultHandler getResultHandler() {
        return this.resultHandler;
    }

}
