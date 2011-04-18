package com.bc.calvalus.ftp;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authentication;
import org.apache.ftpserver.ftplet.AuthenticationFailedException;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.DefaultFtplet;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.FtpletResult;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.impl.DefaultConnectionConfig;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.AnonymousAuthentication;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.ConcurrentLoginPermission;
import org.apache.ftpserver.usermanager.impl.PropertiesUserManager;
import org.apache.ftpserver.usermanager.impl.TransferRatePermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CalvalusFtpServer {
    public static void main(String[] args) throws FtpException {
        PropertiesUserManager userManager = new PropertiesUserManager(new Md5PasswordEncryptor(),
                                                                      new File("ftpserver-users.properties"),
                                                                      "cvop");

        FtpServerFactory serverFactory = new FtpServerFactory();
        serverFactory.setUserManager(userManager);
        serverFactory.setFtplets(createDefaultFtpletMap());
        serverFactory.setConnectionConfig(new DefaultConnectionConfig(true, 3, 16, 16, 3, 10));

        ListenerFactory factory = new ListenerFactory();


        factory.setPort(2121);

        serverFactory.addListener("default", factory.createListener());

        FtpServer server = serverFactory.createServer();

        server.start();
    }

    private static BaseUser createUser(String name, String password, String homeDir) {
        BaseUser user = new BaseUser();
        user.setName(name);
        user.setPassword(password);
        user.setEnabled(true);
        user.setHomeDirectory(homeDir);
        user.setAuthorities(Arrays.asList(new WritePermission(),
                                          new ConcurrentLoginPermission(16, 16)));
        return user;
    }

    private static Map<String, Ftplet> createDefaultFtpletMap() {
        HashMap<String, Ftplet> ftpletMap = new HashMap<String, Ftplet>();
        ftpletMap.put("default", new DefaultFtplet() {
            @Override
            public FtpletResult onDownloadStart(FtpSession session, FtpRequest request) throws FtpException, IOException {
                System.out.println("onDownloadStart: session = " + session + ", request = " + request);
                return super.onDownloadStart(session, request);
            }

            @Override
            public FtpletResult onDownloadEnd(FtpSession session, FtpRequest request) throws FtpException, IOException {
                System.out.println("onDownloadEnd: session = " + session + ", request = " + request);
                return super.onDownloadEnd(session, request);
            }
        });
        return ftpletMap;
    }

   private static class MyAbstractUserManager implements UserManager {

        private final Map<String, User> userMap;


        private MyAbstractUserManager() {
            userMap = new HashMap<String, User>();
        }

        @Override
        public User getUserByName(String name) throws FtpException {
            return userMap.get(name);
        }

        @Override
        public String[] getAllUserNames() throws FtpException {
            return userMap.keySet().toArray(new String[userMap.keySet().size()]);
        }

        @Override
        public void delete(String name) throws FtpException {
            userMap.remove(name);
        }

        @Override
        public void save(User user) throws FtpException {
            userMap.put(user.getName(), user);
        }

        @Override
        public boolean doesExist(String name) throws FtpException {
            return userMap.containsKey(name);
        }

        @Override
        public User authenticate(Authentication authentication) throws AuthenticationFailedException {
            if (authentication instanceof UsernamePasswordAuthentication) {
                UsernamePasswordAuthentication usernamePasswordAuthentication = (UsernamePasswordAuthentication) authentication;
                String name = usernamePasswordAuthentication.getUsername();
                User user = userMap.get(name);
                if (user.getPassword().equals(((UsernamePasswordAuthentication) authentication).getPassword())) {
                    return user;
                }
            } else if (authentication instanceof AnonymousAuthentication) {
                return userMap.get("anonymous");
            }
            throw new AuthenticationFailedException();
        }

        @Override
        public String getAdminName() throws FtpException {
            return "cvop";
        }

        @Override
        public boolean isAdmin(String name) throws FtpException {
            return getAdminName().equals(name);
        }
    }
}
