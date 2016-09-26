package com.bc.calvalus.wps.cmd;

import com.bc.calvalus.wps.exceptions.CommandLineException;
import com.bc.wps.utilities.WpsLogger;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class LdapHelper {

    private static final int DEFAULT_TIMEOUT = 5000;
    private Logger logger = WpsLogger.getLogger();

    public boolean isRegistered(String remoteUserName) {
        return false;
    }

    public void register(String remoteUserName) throws IOException {
        CommandLineWrapper cmd = CommandLineWrapperBuilder.create()
                    .withWorkingDirectoryPath(".")
                    .withCommand("ssh")
                    .withCommandArgs("-t", "-t", "-i", "/home/tomcat/.ssh/id_rsa", "calwps-admin@auth", "useradd", remoteUserName)
                    .withDefaultAsyncCommandLineIO()
                    .build();
        CommandLineIO commandLineIO = cmd.executeAsync();
        CommandLineResultHandler resultHandler = commandLineIO.getResultHandler();
        try {
            resultHandler.waitFor(DEFAULT_TIMEOUT);
        } catch (InterruptedException exception) {
            throw new CommandLineException("Adding user '" + remoteUserName + "'to LDAP took too long. The operation was aborted.");
        }

        if (resultHandler.getExitValue() != 0) {
            System.out.println("====ERROR=======");
            // important information is sometimes also in the outputStream
            for (String outputString : commandLineIO.getOutputStringList()) {
                logger.log(Level.SEVERE, outputString);
            }
            for (String errorString : commandLineIO.getErrorStringList()) {
                logger.log(Level.SEVERE, errorString);
            }
            System.out.println("====ERROR END===");
            throw new CommandLineException("Unable to add user '" + remoteUserName + "' to LDAP : " + commandLineIO.getOutputStream().toString());
        }
        System.out.println("======OUT=======");
        for (String outputString : commandLineIO.getOutputStringList()) {
            logger.log(Level.INFO, outputString);
        }
        System.out.println("====OUT END=====");
    }
}
