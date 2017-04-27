package com.bc.calvalus.wps.cmd;

import com.bc.calvalus.wps.exceptions.CommandLineException;
import com.bc.wps.utilities.PropertiesWrapper;
import com.bc.wps.utilities.WpsLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    List<String> parseLdapIdResponse(List<String> uidGidGroupsLines) {
        // "uid=10230(tep_amarin) gid=10118(calwps) groups=10118(calwps),20009(tep_coreteam)"
        Pattern uidGidGroupsPattern = Pattern.compile(".*groups=(.*)");
        Pattern groupPattern = Pattern.compile("[0-9]*\\((.*)\\)");
        List<String> accu = new ArrayList<>();
        for (String line : uidGidGroupsLines) {
            Matcher groupMatcher = uidGidGroupsPattern.matcher(line);
            if (groupMatcher.matches()) {
                for (String groupIdAndName : groupMatcher.group(1).split(",")) {
                    Matcher matcher = groupPattern.matcher(groupIdAndName);
                    if (matcher.matches()) {
                        accu.add(matcher.group(1));
                    }
                }
            }
        }
        return accu;
    }
}
