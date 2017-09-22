package com.bc.calvalus.rest;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import com.bc.wps.utilities.PropertiesWrapper;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

/**
 * This class handles the production service operations. It has also been extended to include
 * other singleton objects.
 *
 * @author hans
 */
public class CalvalusProductionService implements ServletContextListener {

    private static ServiceContainer serviceContainer = null;

    private static final String DEFAULT_BEAM_BUNDLE = PropertiesWrapper.get("default.beam.bundle");
    private static final String DEFAULT_SNAP_BUNDLE = PropertiesWrapper.get("default.snap.bundle");
    private static final String DEFAULT_CALVALUS_BUNDLE = PropertiesWrapper.get("default.calvalus.bundle");
    private static final String STAGING_DIRECTORY = PropertiesWrapper.get("staging.directory");
    private static final String CATALINA_BASE = System.getProperty("catalina.base");
    private static final String CALREST_ROOT = CATALINA_BASE + PropertiesWrapper.get("calrest.application.path");
    private static final String DEFAULT_CONFIG_PATH = new File(getUserAppDataCalRestDir(), "calvalus.config").getPath();

    private CalvalusProductionService() {
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        System.out.println("******************************************");
        System.out.println("********* Starting Calvalus Rest *********");
        System.out.println("******************************************");
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        System.out.println("******************************************");
        System.out.println("********* Stopping Calvalus Rest *********");
        System.out.println("******************************************");
    }

    synchronized static ServiceContainer getServiceContainerSingleton() throws IOException, ProductionException {
        if (serviceContainer == null) {
            serviceContainer = createServices();
        }
        return serviceContainer;
    }

    private static Map<String, String> getDefaultConfig() {
        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        defaultConfig.put("calvalus.calvalus.bundle", DEFAULT_CALVALUS_BUNDLE);
        defaultConfig.put("calvalus.beam.bundle", DEFAULT_BEAM_BUNDLE);
        defaultConfig.put("calvalus.snap.bundle", DEFAULT_SNAP_BUNDLE);
        defaultConfig.put("calvalus.rest.staging.path", STAGING_DIRECTORY);
        return defaultConfig;
    }

    private static ServiceContainer createServices() throws ProductionException, IOException {
        HadoopServiceContainerFactory productionServiceFactory = new HadoopServiceContainerFactory();
        Map<String, String> defaultConfig = getDefaultConfig();
        Map<String, String> config = ProductionServiceConfig.loadConfig(getConfigFile(), defaultConfig);
        return productionServiceFactory
                    .create(config, getUserAppDataCalRestDir(), new File(CALREST_ROOT, config.get("calvalus.rest.staging.path")));
    }

    private static File getConfigFile() throws FileNotFoundException {
        File configFile;
        try {
            URL calvalusConfigUrl = CalvalusProductionService.class.getClassLoader().getResource("calvalus.config");
            if (calvalusConfigUrl == null) {
                throw new FileNotFoundException("Cannot find calvalus.config file.");
            }
            configFile = new File(calvalusConfigUrl.toURI());
        } catch (URISyntaxException | FileNotFoundException e) {
            configFile = new File(DEFAULT_CONFIG_PATH);
            if (!configFile.exists()) {
                throw new FileNotFoundException("calvalus.config file is not available.");
            }
        }
        return configFile;
    }

    private static File getUserAppDataCalRestDir() {
        String userHome = System.getProperty("user.home");
        return userHome != null ? new File(userHome, ".calrest") : null;
    }
}
