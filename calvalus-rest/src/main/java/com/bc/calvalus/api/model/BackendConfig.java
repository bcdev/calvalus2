package com.bc.calvalus.api.model;

import com.bc.calvalus.production.ProductionServiceConfig;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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
    private String productionServiceFactoryClassName;
    private String stagingPath;
    private String uploadPath;
    private Map<String, String> configMap;

    static BackendConfig singleton = null;
    public synchronized static BackendConfig getConfig(ServletContext context) throws ServletException {
         if (singleton == null) {
             singleton = new BackendConfig(context);
         }
         return singleton;
    }

    public BackendConfig(ServletContext servletContext) throws ServletException {
        String realPath = servletContext.getRealPath(".");
        localContextDir = realPath != null ? new File(realPath) : null;

        configMap = loadConfig(servletContext);

        FileWatcher fileWatcher = new FileWatcher(getConfigFile(servletContext));
        fileWatcher.addListener(() -> {
            configMap = loadConfig(servletContext);
            servletContext.log("Reloaded properties.");
        });
        fileWatcher.start();

        // Init mandatory parameters - absence is not acceptable.
        productionServiceFactoryClassName = getProperty("calvalus.portal.productionServiceFactory.class");
        stagingPath = getProperty("calvalus.portal.staging.path");
        uploadPath = getProperty("calvalus.portal.upload.path");
    }

    public Map<String, String> getConfigMap() {
        return configMap;
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

    public File getLocalAppDataDir() {
        File userAppDataDir = ProductionServiceConfig.getUserAppDataDir();
        if (userAppDataDir != null)  {
             return userAppDataDir;
        }
        return getLocalContextDir();
    }

    private static Map<String, String> loadConfig(ServletContext servletContext) {
        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        File configFile = getConfigFile(servletContext);
        try {
            return ProductionServiceConfig.loadConfig(configFile, defaultConfig);
        } catch (IOException e) {
            servletContext.log("I/O problem while reading Calvalus configuration file " + configFile, e);
            return Collections.emptyMap();
        }

    }

    private String getProperty(String name) throws ServletException {
        String property = configMap.get(name);
        if (property == null) {
            throw new ServletException(String.format("Missing configuration parameter '%s'", name));
        }
        return property;
    }

    static File getConfigFile(ServletContext servletContext) {
        servletContext.log("searching Config File");
        String configPath = System.getProperty(PARAM_NAME_CONFIG_FILE);
        if (configPath == null) {
            servletContext.log(String.format("System property '%s' not set", PARAM_NAME_CONFIG_FILE));
            configPath = servletContext.getInitParameter(PARAM_NAME_CONFIG_FILE);
            if (configPath == null) {
                servletContext.log(String.format("ServletContext property '%s' not set", PARAM_NAME_CONFIG_FILE));
                configPath = DEFAULT_PARAM_VALUE_CONFIG_FILE;
                servletContext.log(String.format("Context parameter '%s' not set, assuming file '%s'", PARAM_NAME_CONFIG_FILE, configPath));
            }
        }
        servletContext.log(String.format("Config File is '%s'", configPath));
        File configFile = new File(configPath);
        if (!configFile.isAbsolute()) {
            configFile = new File(servletContext.getRealPath("."), configFile.getPath());
            servletContext.log(String.format("making Config File absolute '%s'", configPath));
        }
        return configFile;
    }

}
