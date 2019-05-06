package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import com.bc.wps.utilities.PropertiesWrapper;
import com.bc.wps.utilities.WpsServletContainer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * This class handles the production service operations. It has also been extended to include
 * other singleton objects.
 *
 * @author hans
 */
public class CalvalusProductionService implements ServletContextListener {

    private static ServiceContainer serviceContainer = null;
    private static ReportingHandler reportingHandler = null;
    private static Timer statusObserver;
    private static Map<String, Integer> userProductionMap;
    private static Set<String> remoteUserSet;

    private static final String DEFAULT_BEAM_BUNDLE = PropertiesWrapper.get("default.beam.bundle");
    private static final String DEFAULT_SNAP_BUNDLE = PropertiesWrapper.get("default.snap.bundle");
    private static final String DEFAULT_CALVALUS_BUNDLE = PropertiesWrapper.get("default.calvalus.bundle");
    private static final String STAGING_DIRECTORY = PropertiesWrapper.get("staging.directory");
    private static final String CATALINA_BASE = System.getProperty("catalina.base");
    private static final String CALWPS_ROOT = CATALINA_BASE + PropertiesWrapper.get("wps.application.path");
    private static final String DEFAULT_CONFIG_PATH = new File(getUserAppDataCalWpsDir(), "calvalus.config").getPath();

    private CalvalusProductionService() {
    }

    public synchronized static Timer getStatusObserverSingleton() {
        if (statusObserver == null) {
            WpsServletContainer.addServletContextListener(new CalvalusProductionService());
            //statusObserver = new Timer("StatusObserver" + new Date().toString(), true);
            statusObserver = serviceContainer.getProductionService().getProcessingService().getTimer();
        }
        return statusObserver;
    }

    public synchronized static Set<String> getRemoteUserSet() {
        if (remoteUserSet == null) {
            remoteUserSet = new HashSet<>();
        }
        return remoteUserSet;
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        System.out.println("*****************************************");
        System.out.println("********* Starting Calvalus WPS *********");
        System.out.println("*****************************************");
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        System.out.println("*****************************************");
        System.out.println("********* Stopping Calvalus WPS *********");
        System.out.println("*****************************************");
        System.out.println("Shutting down statusObserver...");
        synchronized (CalvalusProductionService.class) {
            if (reportingHandler != null) {
                serviceContainer.getProductionService().deleteObserver(reportingHandler);
            }
            if (statusObserver != null) {
                statusObserver.cancel();
            }
        }
    }

    synchronized static ServiceContainer getServiceContainerSingleton() throws IOException, ProductionException {
        if (serviceContainer == null) {
            serviceContainer = createServices();
        }
        return serviceContainer;
    }

    synchronized static Map<String, Integer> getUserProductionMap() {
        if (userProductionMap == null) {
            userProductionMap = new HashMap<>();
        }
        return userProductionMap;
    }

    static Map<String, String> getDefaultConfig() {
        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        defaultConfig.put("calvalus.calvalus.bundle", DEFAULT_CALVALUS_BUNDLE);
        defaultConfig.put("calvalus.beam.bundle", DEFAULT_BEAM_BUNDLE);
        defaultConfig.put("calvalus.snap.bundle", DEFAULT_SNAP_BUNDLE);
        defaultConfig.put("calvalus.wps.staging.path", STAGING_DIRECTORY);
        return defaultConfig;
    }

    private static ServiceContainer createServices() throws ProductionException, IOException {
        HadoopServiceContainerFactory productionServiceFactory = new HadoopServiceContainerFactory();
        Map<String, String> defaultConfig = getDefaultConfig();
        Map<String, String> config = ProductionServiceConfig.loadConfig(getConfigFile(), defaultConfig);
        ServiceContainer serviceContainer = productionServiceFactory
                .create(config, getUserAppDataCalWpsDir(), new File(CALWPS_ROOT, config.get("calvalus.wps.staging.path")));
        String reportPath = PropertiesWrapper.get("wps.reporting.db.path");
        if (reportPath != null) {
            reportingHandler = ReportingHandler.createReportHandler(serviceContainer.getProductionService(), reportPath);
            serviceContainer.getProductionService().addObserver(reportingHandler);
        }
        return serviceContainer;
    }

    public static File getConfigFile() throws FileNotFoundException {
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

    private static File getUserAppDataCalWpsDir() {
        String userHome = System.getProperty("user.home");
        return userHome != null ? new File(userHome, ".calwps") : null;
    }
}
