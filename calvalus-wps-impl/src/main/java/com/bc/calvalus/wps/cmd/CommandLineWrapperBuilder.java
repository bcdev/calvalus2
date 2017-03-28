package com.bc.calvalus.wps.cmd;

import com.bc.wps.utilities.PropertiesWrapper;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * @author hans
 */
public class CommandLineWrapperBuilder {

    private static final int DEFAULT_TIMEOUT = PropertiesWrapper.getInteger("default.cmd.timeout");

    private String workingDirectoryPath;
    private String command;
    private String[] commandArgs;
    private CommandLineIO commandLineIO;
    private long timeout;
    private CommandLineResultHandler resultHandler;

    public static CommandLineWrapperBuilder create() {
        return new CommandLineWrapperBuilder();
    }

    public CommandLineWrapper build() {
        return new CommandLineWrapper(this);
    }

    public CommandLineWrapperBuilder withWorkingDirectoryPath(String workingDirectoryPath) {
        this.workingDirectoryPath = workingDirectoryPath;
        return this;
    }

    public CommandLineWrapperBuilder withCommand(String command) {
        this.command = command;
        return this;
    }

    public CommandLineWrapperBuilder withCommandArgs(String... commandArgs) {
        this.commandArgs = commandArgs;
        return this;
    }

    public CommandLineWrapperBuilder withCommandLineIO(CommandLineIO commandLineIO) {
        this.commandLineIO = commandLineIO;
        return this;
    }

    public CommandLineWrapperBuilder withDefaultSyncCommandLineIO() {
        this.commandLineIO = new CommandLineIO(new ByteArrayOutputStream(), new ByteArrayOutputStream());
        return this;
    }

    public CommandLineWrapperBuilder withDefaultAsyncCommandLineIO() {
        CommandLineResultHandler resultHandler = new CommandLineResultHandler(new ExecuteWatchdog(DEFAULT_TIMEOUT));
        this.commandLineIO = new CommandLineIO(new ByteArrayOutputStream(), new ByteArrayOutputStream(), resultHandler);
        return this;
    }

    public CommandLineWrapperBuilder withTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public CommandLineWrapperBuilder withResultHandler(CommandLineResultHandler resultHandler) {
        this.resultHandler = resultHandler;
        return this;
    }

    public String getWorkingDirectoryPath() {
        return workingDirectoryPath;
    }

    public String getCommand() {
        return command;
    }

    public String[] getCommandArgs() {
        return commandArgs;
    }

    public CommandLineIO getCommandLineIO() {
        return commandLineIO;
    }

    public long getTimeout() {
        return timeout;
    }

    public CommandLineResultHandler getResultHandler() {
        return resultHandler;
    }
}
