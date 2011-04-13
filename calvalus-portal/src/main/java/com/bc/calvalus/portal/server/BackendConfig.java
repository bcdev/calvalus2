package com.bc.calvalus.portal.server;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Configuration of the backend service of the portal.
 *
 * @author Norman
 */
public class BackendConfig {

    private static final String CALVALUS_PROPERTIES = "calvalus.properties";
    private final File localContextDir;
    private final String stagingPath;
    private final String uploadPath;
    private final Properties properties;

    public BackendConfig(ServletContext servletContext) throws ServletException {
        Properties properties = new Properties();
        loadConfig(servletContext, properties);
        overwriteConfig(properties);
        this.properties = properties;
        this.stagingPath = getProperty("calvalus.portal.staging.path");
        this.uploadPath = getProperty("calvalus.portal.upload.path");
        this.localContextDir = new File(servletContext.getRealPath("."));
    }

    public String getProperty(String name) throws ServletException {
        String property = properties.getProperty(name);
        if (property == null) {
            throw new ServletException(String.format("Missing configuration parameter '%s'", name));
        }
        return property;
    }

    public String getProperty(String name, String defaultValue) {
        return properties.getProperty(name, defaultValue);
    }

    public String getStagingPath() {
        return stagingPath;
    }

    public String getUploadPath() {
        return uploadPath;
    }

    public File getLocalUploadDir() {
        return new File(getLocalContextDir(), getUploadPath());
    }

    public File getLocalStagingDir() {
        return new File(getLocalContextDir(), getStagingPath());
    }

    public File getLocalContextDir() {
        return localContextDir;
    }

    private static void loadConfig(ServletContext servletContext, Properties calvalusConfig) {
        String calvalusConfigPath = servletContext.getInitParameter(CALVALUS_PROPERTIES);
        if (calvalusConfigPath == null) {
            calvalusConfigPath = "config/calvalus.properties";
            servletContext.log(String.format("Context parameter '%s' not set, assuming file '%s'", CALVALUS_PROPERTIES, calvalusConfigPath));
        }
        File calvalusConfigFile = new File(servletContext.getRealPath("."), calvalusConfigPath);
        try {
            FileReader reader = new FileReader(calvalusConfigFile);
            try {
                calvalusConfig.load(reader);
            } finally {
                reader.close();
            }
        } catch (IOException e) {
            servletContext.log("I/O problem while reading Calvalus configuration file " + calvalusConfigFile, e);
        }
    }

    private static void overwriteConfig(Properties properties) {
        Set<Map.Entry<Object,Object>> entries = System.getProperties().entrySet();
        for (Map.Entry<Object, Object> entry: entries){
            if (entry.getKey().toString().startsWith("calvalus.")) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
