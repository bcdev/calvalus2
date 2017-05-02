package com.bc.calvalus.urban.ws;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;

import static com.bc.calvalus.urban.LoadProperties.getInstance;

/**
 * @author muhammad.bc.
 */
public class CopyWpsRemoteFile {
    private static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    private static final String HASH_KNOWN_HOSTS = "HashKnownHosts";
    private static final String YES = "yes";
    private static final String NO = "no";
    private static final String EXEC = "exec";
    private static final String privateKeyPath = getInstance().getRemotePrivateKeyPath();
    private static final String remotePath = getInstance().getRemoteFilePath();
    private static final String remoteHostName = getInstance().getRemoteHostName();
    private static final String remoteUserName = getInstance().getRemoteUserName();
    private static final Optional<String> passKeyPath = Optional.ofNullable(getInstance().getRemotePassphrase());
    private Channel channel = null;
    private Session session = null;


    public BufferedReader readRemoteFile() throws IOException, JSchException {
        if (session == null) {
            session = getRemoteSession();
        }
        session.connect();
        String commandToExec = String.format("tail -100f %scalvalus-wps-reporting.report", remotePath);
        Channel channel = session.openChannel(EXEC);
        ((ChannelExec) channel).setCommand(commandToExec);
        channel.setInputStream(null);
        Optional<InputStream> inputStreamOptional = Optional.ofNullable(channel.getInputStream());
        channel.connect();
        inputStreamOptional.orElseThrow(() -> new IOException("Check the remote file path"));
        InputStreamReader inputStreamReader = new InputStreamReader(inputStreamOptional.get(), "utf-8");
        return new BufferedReader(inputStreamReader);
    }

    private Session getRemoteSession() throws JSchException {
        JSch jsch = new JSch();
        JSch.setConfig(STRICT_HOST_KEY_CHECKING, NO);
        JSch.setConfig(HASH_KNOWN_HOSTS, YES);
        if (passKeyPath.isPresent()) {
            jsch.addIdentity(privateKeyPath, passKeyPath.get());
        } else {
            jsch.addIdentity(privateKeyPath);
        }
        return jsch.getSession(remoteUserName, remoteHostName, 22);
    }
}
