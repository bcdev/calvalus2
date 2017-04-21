package com.bc.calvalus.urban.ws;

import com.bc.wps.utilities.PropertiesWrapper;
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

/**
 * @author muhammad.bc.
 */
class CopyReportFile {
    private static final String EXEC = "exec";
    private static final String remotePath = PropertiesWrapper.get("remote.file.path");
    private static final String privateKeyPath = PropertiesWrapper.get("remote.private.key.path");
    private static final String hostName = PropertiesWrapper.get("remote.host.name");
    private static final String userName = PropertiesWrapper.get("remote.user.name");
    private static final Optional<String> passKeyPath = Optional.ofNullable(PropertiesWrapper.get("remote.passphrase"));
    private Channel channel = null;
    private Session session = null;

    Optional<BufferedReader> readFile(String fileToCopy) throws IOException, JSchException {
        Optional<BufferedReader> bufferedReader = Optional.empty();

        session = getRemoteSession();
        session.connect();
        String commandToExec = String.format("tail -100f %scalvalus-wps-%s.report", remotePath, fileToCopy);
        channel = session.openChannel(EXEC);
        ((ChannelExec) channel).setCommand(commandToExec);
        channel.setInputStream(null);
        Optional<InputStream> inputStreamOptional = Optional.ofNullable(channel.getInputStream());
        channel.connect();
        inputStreamOptional.orElseThrow(() -> new IOException("Check the remote file path"));
        InputStreamReader inputStreamReader = new InputStreamReader(inputStreamOptional.get(), "utf-8");
        bufferedReader = Optional.ofNullable(new BufferedReader(inputStreamReader));
        return bufferedReader;
    }

    private Session getRemoteSession() throws JSchException, IOException {
        JSch jsch = new JSch();
        JSch.setConfig("StrictHostKeyChecking", "no");
        JSch.setConfig("HashKnownHosts", "yes");
        if (passKeyPath.isPresent()) {
            jsch.addIdentity(privateKeyPath, passKeyPath.get());
        } else {
            jsch.addIdentity(privateKeyPath);
        }
        return jsch.getSession(userName, hostName, 22);
    }
}
