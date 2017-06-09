package com.bc.calvalus.reporting.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;

/**
 * @author muhammad.bc.
 */
public class LoadProperties {
    private String calvalusReportUrl;
    private String accountServerUrl;
    private String userName;
    private String platForm;
    private String password;
    private String hostName;
    private String initStartTime;
    private String logAccountMessagePath;
    private String remotePrivateKeyPath;
    private String remoteFilePath;
    private String remoteHostName;
    private String remoteUserName;
    private String remotePassphrase;
    private static LoadProperties loadProperties;
    private String cursorFileLocation;


    private LoadProperties() {
        try (InputStream resourceAsStream
                     = LoadProperties.class
                .getClassLoader().getResourceAsStream("urbanTEP.properties")) {
            Properties properties = new Properties();
            properties.load(resourceAsStream);


            calvalusReportUrl = (String) properties.get("calvalus.reporting.url");
            //-- Account info
            accountServerUrl = (String) properties.get("account.server.url");
            userName = (String) properties.get("account.server.username");
            platForm = (String) properties.get("account.server.platform");
            password = (String) properties.get("account.server.password");
            logAccountMessagePath = (String) properties.get("account.log.send.path");

            // Remote file copying
            remotePrivateKeyPath = (String) properties.get("remote.private.key.path");
            remoteFilePath = (String) properties.get("remote.file.path");
            remoteHostName = (String) properties.get("remote.host.name");
            remoteUserName = (String) properties.get("remote.user.name");
            remotePassphrase = (String) properties.get("remote.passphrase");


            hostName = (String) properties.get("host.name");
            cursorFileLocation = (String) properties.get("cursor.url");
            initStartTime = (String) properties.get("start.date.time.test");
        } catch (IOException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, String.format("Exception in load properties settings %s",
                                                                       e.getMessage()));
        }
    }

    public static LoadProperties getInstance(){
        if (loadProperties == null) {
            loadProperties = new LoadProperties();
        }
        return loadProperties;
    }

    public String getCalvalusReportUrl() {
        return calvalusReportUrl;
    }

    public String getAccountServerUrl() {
        return accountServerUrl;
    }

    public String getUserName() {
        return userName;
    }

    public String getPlatForm() {
        return platForm;
    }

    public String getPassword() {
        return password;
    }

    public String getHostName() {
        return hostName;
    }

    public String getInitStartTime() {
        return initStartTime;
    }

    public String getLogAccountMessagePath() {
        return logAccountMessagePath;
    }

    public String getRemotePrivateKeyPath() {
        return remotePrivateKeyPath;
    }

    public String getRemoteFilePath() {
        return remoteFilePath;
    }

    public String getRemoteHostName() {
        return remoteHostName;
    }

    public String getRemoteUserName() {
        return remoteUserName;
    }

    public String getRemotePassphrase() {
        return remotePassphrase;
    }

    public static LoadProperties getLoadProperties() {
        return loadProperties;
    }

    public String getCursorFileLocation() {
        return cursorFileLocation;
    }
}

