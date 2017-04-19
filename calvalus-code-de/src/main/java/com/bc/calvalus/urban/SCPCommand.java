package com.bc.calvalus.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.wps.utilities.PropertiesWrapper;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author muhammad.bc.
 */
public class SCPCommand extends SSHSession {
    private static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    private static final String NO = "no";
    private static final String PASSWORD = "password";
    private static final String REMOTE_FILE_PATH = "remote.file.path";
    private static final String EXEC = "exec";
    private Logger logger = CalvalusLogger.getLogger();
    private Optional<Session> remoteSession = Optional.empty();


    public Optional<Session> getSession() {
        if (!remoteSession.isPresent()) {
            remoteSession = getRemoteSession();
        }
        return remoteSession;
    }

    public Optional<BufferedReader> readFile(Session session) {
        Optional<BufferedReader> readerOptional = Optional.empty();
        try {
            String fileToCopy = PropertiesWrapper.get(REMOTE_FILE_PATH);
            String password = PropertiesWrapper.get(PASSWORD);
            session.setHostKeyAlias("life200");
            session.setConfig(STRICT_HOST_KEY_CHECKING, "no");
            session.setPassword(password);
            session.connect();
            String command = String.format("cat %s", fileToCopy);
            Channel channel = session.openChannel(EXEC);
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            Optional<InputStream> inputStreamOptional = Optional.ofNullable(channel.getInputStream());
            channel.connect();
            inputStreamOptional.orElseThrow(() -> new IOException("Check the remote file path"));
            InputStreamReader inputStreamReader = new InputStreamReader(inputStreamOptional.get(), "utf-8");
            readerOptional = Optional.ofNullable(new BufferedReader(inputStreamReader));
            session.disconnect();
            channel.disconnect();

        } catch (JSchException | IOException e) {
            e.printStackTrace();
        }
        return readerOptional;
    }

    private Optional<Session> getRemoteSession() {
        Optional<Session> remoteSession = Optional.empty();
        String pubKeyPath = PropertiesWrapper.get("public.key.path");
        String privateKeyPath = PropertiesWrapper.get("private.key.path");
        String hostName = PropertiesWrapper.get("host.name");
        String userName = PropertiesWrapper.get("user.name");
        Optional<String> passKeyPath = Optional.ofNullable(PropertiesWrapper.get("passphrase"));

        try {
            if (passKeyPath.isPresent()) {
                remoteSession = getRemoteSession(privateKeyPath, passKeyPath.get(), userName, hostName);
            } else {
                remoteSession = getRemoteSession(privateKeyPath, pubKeyPath, userName, hostName);
            }
        } catch (JSchException e) {
            logger.log(Level.SEVERE, e.getMessage() + "Ensure the private key is in open ssh format.");
        }
        return remoteSession;
    }
}
