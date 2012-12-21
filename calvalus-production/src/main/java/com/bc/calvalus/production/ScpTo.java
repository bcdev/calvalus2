package com.bc.calvalus.production;/* -*-mode:java; c-basic-offset:2; indent-tabs-mode:nil -*- */
/**
 * This program will demonstrate the file transfer from local to remote.
 *   $ CLASSPATH=.:../build javac ScpTo.java
 *   $ CLASSPATH=.:../build java ScpTo file1 user@remotehost:file2
 * You will be asked passwd. 
 * If everything works fine, a local file 'file1' will copied to
 * 'file2' on 'remotehost'.
 *
 */

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * The basis for this class is an example which can be found at
 * <a href="http://www.jcraft.com/jsch/examples/ScpTo.java.html">www.jcraft.com</a>.
 * Api-doc for the JSch project can be found <a href="http://epaul.github.com/jsch-documentation/javadoc/">here</a>.
 */
public class ScpTo {

    private final String username;
    private final String host;
    private final JSch jsch;
    private final boolean preserveTimeStamp;
    private Session session;

    public ScpTo(String username, String host) throws ScpException {
        this.username = username;
        this.host = host;
        this.preserveTimeStamp = false;
        this.jsch = new JSch();
        JSch.setConfig("StrictHostKeyChecking", "no");
        JSch.setConfig("HashKnownHosts", "yes");
    }

    public void connect() throws ScpException {
        if (session == null) {
            try {
                File publicKeyFile = new File(System.getProperty("user.home"), ".ssh/id_rsa");
                jsch.addIdentity(publicKeyFile.getCanonicalPath());
                session = jsch.getSession(username, host, 22);
                session.connect();
            } catch (Exception e) {
                throw new ScpException(e);
            }
        }
    }

    public void disconnect() {
        if (session != null) {
            session.disconnect();
        }
    }

    public void copy(String localFilePath, String remoteFilePath) throws IOException, ScpException {
        // exec 'scp -t rfile' remotely
        String command = "scp " + (preserveTimeStamp ? "-p" : "") + " -t " + remoteFilePath;
        Channel channel = null;

        try {
            channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            // get I/O streams for remote scp
            OutputStream out = channel.getOutputStream();
            InputStream in = channel.getInputStream();
            try {
                channel.connect();
                checkAck(in);


                File localFile = new File(localFilePath);
                if (preserveTimeStamp) {
                    sendTimeStamp(out, localFile);
                    checkAck(in);
                }

                sendSingleFileScpCommand(out, localFile);
                checkAck(in);

                // send a content of the local file
                sendFileContent(out, localFile);
                checkAck(in);
            } finally {
                out.close();
                in.close();
            }
        } catch (JSchException e) {
            throw new ScpException(e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
        }
    }

    private void sendSingleFileScpCommand(OutputStream out, File localFile) throws IOException {
        String command;// send "C0644 filesize filename", where filename should not include '/'
        long filesize = localFile.length();
        command = "C0644 " + filesize + " ";
        command += localFile.getName() + "\n";
        out.write(command.getBytes());
        out.flush();
    }

    private void sendTimeStamp(OutputStream out, File localFile) throws IOException {
        String command;
        command = "T " + (localFile.lastModified() / 1000) + " 0";
        // The access time should be sent here,
        // but it is not accessible with JavaAPI ;-<
        command += (" " + (localFile.lastModified() / 1000) + " 0\n");
        out.write(command.getBytes());
        out.flush();
    }

    private void sendFileContent(OutputStream out, File localFile) throws IOException {
        byte[] buf;
        FileInputStream fis = new FileInputStream(localFile);
        try {
            buf = new byte[1024];
            while (true) {
                int len = fis.read(buf, 0, buf.length);
                if (len <= 0) {
                    break;
                }
                out.write(buf, 0, len);
            }
        } finally {
            fis.close();
        }
        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();
    }

    private static void checkAck(InputStream in) throws IOException {
        int b = in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //          -1
        if (b == 1 || b == 2) {
            StringBuilder sb = new StringBuilder();
            int c;
            do {
                c = in.read();
                sb.append((char) c);
            }
            while (c != '\n');
            throw new IOException(sb.toString());
        }
    }

    public static void main(String[] arg) {
        if (arg.length != 2) {
            System.err.println("usage: java ScpTo file1 user@remotehost:file2");
            System.exit(-1);
        }

        try {
            String localFilePath = arg[0];
            String user = arg[1].substring(0, arg[1].indexOf('@'));
            arg[1] = arg[1].substring(arg[1].indexOf('@') + 1);
            String host = arg[1].substring(0, arg[1].indexOf(':'));
            String remoteFilePath = arg[1].substring(arg[1].indexOf(':') + 1);

            ScpTo scpTo = new ScpTo(user, host);
            scpTo.connect();

            scpTo.copy(localFilePath, remoteFilePath);

            scpTo.disconnect();

        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
    }

}
