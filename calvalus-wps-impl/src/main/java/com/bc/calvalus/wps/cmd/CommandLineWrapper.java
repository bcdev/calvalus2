package com.bc.calvalus.wps.cmd;

import com.bc.calvalus.wps.exceptions.CommandLineException;
import com.bc.wps.utilities.WpsLogger;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CommandLineWrapper {

    private Logger logger = WpsLogger.getLogger();

    private String workingDirectoryPath;
    private String command;
    private String[] commandArgs;
    private CommandLineIO commandLineIO;
    private long timeout;

    public CommandLineWrapper(CommandLineWrapperBuilder builder) {
        this.workingDirectoryPath = builder.getWorkingDirectoryPath();
        this.command = builder.getCommand();
        this.commandArgs = builder.getCommandArgs();
        this.commandLineIO = builder.getCommandLineIO();
        this.timeout = builder.getTimeout();
    }

    public CommandLineIO executeAsync() throws IOException {
        CommandLine cmdLine = CommandLine.parse(command);
        for (String arg : commandArgs) {
            cmdLine.addArgument(arg);
        }
        DefaultExecutor executor = new DefaultExecutor();
        executor.setWorkingDirectory(new File(workingDirectoryPath));
        PumpStreamHandler streamHandler = new PumpStreamHandler(commandLineIO.getOutputStream(), commandLineIO.getErrorStream());
        executor.setStreamHandler(streamHandler);
        executor.execute(cmdLine, commandLineIO.getResultHandler());
        return commandLineIO;
    }

    public CommandLineIO executeSync() throws IOException {
        CommandLine cmdLine = CommandLine.parse(command);
        for (String arg : commandArgs) {
            cmdLine.addArgument(arg);
        }
        DefaultExecutor executor = new DefaultExecutor();
        executor.setWorkingDirectory(new File(workingDirectoryPath));
        PumpStreamHandler streamHandler = new PumpStreamHandler(commandLineIO.getOutputStream(), commandLineIO.getErrorStream());
        ExecuteWatchdog watchDog = new ExecuteWatchdog(timeout);
        executor.setStreamHandler(streamHandler);
        executor.setWatchdog(watchDog);
        try {
            executor.execute(cmdLine);
        } catch (IOException exception) {
            // important information is sometimes also in the outputStream
            for (String outputString : commandLineIO.getOutputStringList()) {
                logger.log(Level.SEVERE, outputString);
            }
            for (String errorString : commandLineIO.getErrorStringList()) {
                logger.log(Level.SEVERE, errorString);
            }
            StringBuilder sb = new StringBuilder();
            sb.append(command);
            for (String arg : commandArgs) {
                sb.append(" ");
                sb.append(arg);
            }
            logger.log(Level.SEVERE, "command : " + sb.toString(), exception);
            throw new CommandLineException("Error when executing command line.");
        }
        return commandLineIO;
    }
}
