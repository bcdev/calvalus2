package com.bc.calvalus.wps.cmd;

import com.bc.calvalus.wps.exceptions.CommandLineException;
import com.bc.wps.utilities.PropertiesWrapper;
import com.bc.wps.utilities.WpsLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class LdapHelper {

    private static final int DEFAULT_TIMEOUT = PropertiesWrapper.getInteger("default.cmd.timeout");
    private static final String ALLOWABLE_GROUP = PropertiesWrapper.get("ldap.allowed.group");

    private Logger logger = WpsLogger.getLogger();

    public boolean isRegistered(String remoteUserName) throws IOException {
        String ldapServerName = PropertiesWrapper.get("ldap.server.name");
        String ldapSshKeyFile = PropertiesWrapper.get("ldap.ssh.key");
        CommandLineWrapper cmd = CommandLineWrapperBuilder.create()
                    .withWorkingDirectoryPath(".")
                    .withCommand("ssh")
                    .withCommandArgs("-t", "-t", "-i", ldapSshKeyFile, ldapServerName, "id", remoteUserName)
                    .withDefaultAsyncCommandLineIO()
                    .build();
        CommandLineIO commandLineIO = cmd.executeAsync();
        CommandLineResultHandler resultHandler = commandLineIO.getResultHandler();
        try {
            resultHandler.waitFor(DEFAULT_TIMEOUT);
        } catch (InterruptedException exception) {
            throw new CommandLineException("Checking user '" + remoteUserName + "' from LDAP took too long. The operation was aborted.");
        }
        if (resultHandler.getExitValue() == 0) {
            List<String> groupList = parseLdapIdResponse(commandLineIO.getOutputStringList());
            for (String group : groupList) {
                if (ALLOWABLE_GROUP.equals(group)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void register(String remoteUserName) throws IOException {
        String ldapServerName = PropertiesWrapper.get("ldap.server.name");
        String ldapSshKeyFile = PropertiesWrapper.get("ldap.ssh.key");
        CommandLineWrapper cmd = CommandLineWrapperBuilder.create()
                    .withWorkingDirectoryPath(".")
                    .withCommand("ssh")
                    .withCommandArgs("-t", "-t", "-i", ldapSshKeyFile, ldapServerName, "useradd", remoteUserName)
                    .withDefaultAsyncCommandLineIO()
                    .build();
        CommandLineIO commandLineIO = cmd.executeAsync();
        CommandLineResultHandler resultHandler = commandLineIO.getResultHandler();
        try {
            resultHandler.waitFor(DEFAULT_TIMEOUT);
        } catch (InterruptedException exception) {
            throw new CommandLineException("Adding user '" + remoteUserName + "' to LDAP took too long. The operation was aborted.");
        }

        if (resultHandler.getExitValue() != 0) {
            // important information is sometimes also in the outputStream
            for (String outputString : commandLineIO.getOutputStringList()) {
                logger.log(Level.SEVERE, outputString);
            }
            for (String errorString : commandLineIO.getErrorStringList()) {
                logger.log(Level.SEVERE, errorString);
            }
            throw new CommandLineException("Unable to add user '" + remoteUserName + "' to LDAP : " + commandLineIO.getOutputStream().toString());
        }
        for (String outputString : commandLineIO.getOutputStringList()) {
            logger.log(Level.INFO, outputString);
        }
    }

    private List<String> parseLdapIdResponse(List<String> outputStringList) {
        List<String> groupList = new ArrayList<>();
        String ldapGroups = "(?=groups=.*\\()";
        for (String outputString : outputStringList) {
            String[] groups = outputString.split(ldapGroups);
            for (String group : groups) {
                if (group.contains("groups")) {
                    groupList.add(group.replaceAll("groups=\\d{1,5}\\(", "").replaceAll("\\)\\s*", ""));
                }
            }
        }
        return groupList;
    }
}
