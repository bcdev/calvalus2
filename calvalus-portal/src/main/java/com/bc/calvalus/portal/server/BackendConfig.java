package com.bc.calvalus.portal.server;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration of the backend service of the portal.
 * <p/>
 * The configuration is read from a Java properties file.
 * The properties file may be either given by a system property 'calvalus.properties' or the
 * servlet context (init) parameter with the same name.
 * If neither is given, the path '${webapp}/config/calvalus.properties' is assumed for the config file.
 * A given system property has preference over the servlet context parameter.
 * <p/>
 * After loading the properties from the configuration file, all system properties whose name starts with 'calvalus.'
 * are also added to the configuration.
 *
 * @author Norman
 */
public class BackendConfig {

    private static final String PARAM_NAME_CONFIG_FILE = "calvalus.properties";
    private static final String DEFAULT_PARAM_VALUE_CONFIG_FILE = "config/calvalus.properties";

    private final File localContextDir;
    private final String productionServiceFactoryClassName;
    private final String stagingPath;
    private final String uploadPath;
    private final Map<String, String> configMap;

    public BackendConfig(ServletContext servletContext) throws ServletException {
        Map<String, String> configMap = loadConfig(servletContext);
        overwriteConfigWithSystemProperties(configMap);

        this.configMap = configMap;
        this.localContextDir = new File(servletContext.getRealPath("."));

        // Init mandatory parameters - absence is not acceptable.
        this.productionServiceFactoryClassName = getProperty("calvalus.portal.productionServiceFactory.class");
        this.stagingPath = getProperty("calvalus.portal.staging.path");
        this.uploadPath = getProperty("calvalus.portal.upload.path");
    }

    public Map<String, String> getConfigMap() {
        return configMap;
    }

    public String getProperty(String name) throws ServletException {
        String property = configMap.get(name);
        if (property == null) {
            throw new ServletException(String.format("Missing configuration parameter '%s'", name));
        }
        return property;
    }

    public String getProperty(String name, String defaultValue) {
        String value = configMap.get(name);
        return value != null ? value : defaultValue;
    }

    public String getProductionServiceFactoryClassName() {
        return productionServiceFactoryClassName;
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

    private static Map<String, String> loadConfig(ServletContext servletContext) {
        File configFile = getConfigFile(servletContext);
        Map<String, String> calvalusConfig = new HashMap<String, String>();
        try {
            FileReader reader = new FileReader(configFile);
            Properties properties = new Properties();
            try {
                properties.load(reader);
                overwriteConfigWithProperties(calvalusConfig, properties, null);
            } finally {
                reader.close();
            }
        } catch (IOException e) {
            servletContext.log("I/O problem while reading Calvalus configuration file " + configFile, e);
        }
        return calvalusConfig;
    }

    static File getConfigFile(ServletContext servletContext) {
        String configPath = System.getProperty(PARAM_NAME_CONFIG_FILE);
        if (configPath == null) {
            configPath = servletContext.getInitParameter(PARAM_NAME_CONFIG_FILE);
            if (configPath == null) {
                configPath = DEFAULT_PARAM_VALUE_CONFIG_FILE;
                servletContext.log(String.format("Context parameter '%s' not set, assuming file '%s'", PARAM_NAME_CONFIG_FILE, configPath));
            }
        }
        File configFile = new File(configPath);
        if (!configFile.isAbsolute()) {
            configFile = new File(servletContext.getRealPath("."), configFile.getPath());
        }
        return configFile;
    }

    static void overwriteConfigWithSystemProperties(Map<String, String> calvalusConfig) {
        overwriteConfigWithProperties(calvalusConfig, System.getProperties(), "calvalus.");
    }

    static void overwriteConfigWithProperties(Map<String, String> calvalusConfig, Properties properties, String prefix) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String name = entry.getKey().toString();
            if (prefix != null) {
                if (name.startsWith(prefix)) {
                    calvalusConfig.put(name, properties.getProperty(name));
                }
            } else {
                calvalusConfig.put(name, properties.getProperty(name));
            }
        }
    }
}
