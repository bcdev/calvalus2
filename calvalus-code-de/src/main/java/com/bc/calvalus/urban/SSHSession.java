package com.bc.calvalus.urban;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.util.Optional;

/**
 * @author muhammad.bc.
 */
public abstract class SSHSession {

    protected Optional<Session> getRemoteSession(String privateKey, String passPhase, String userName, String hostName) throws JSchException {
        JSch jSch = new JSch();
        jSch.addIdentity(privateKey, passPhase.getBytes());
        return Optional.ofNullable(jSch.getSession(userName, hostName));
    }

    protected Optional<Session> getRemoteSession(String privateKey, String userName, String hostName) throws JSchException {
        JSch jSch = new JSch();
        jSch.addIdentity(privateKey);
        return Optional.ofNullable(jSch.getSession(userName, hostName));
    }
}