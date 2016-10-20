package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.wps.utilities.PropertiesWrapper;
import com.bc.wps.utilities.WpsLogger;
import com.bc.wps.utilities.WpsServletContainer;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class GpfProductionService implements ServletContextListener {

    private static final String STAGING_DIRECTORY = PropertiesWrapper.get("staging.directory");
    private static final String CATALINA_BASE = System.getProperty("catalina.base");
    private static final String CALWPS_ROOT = CATALINA_BASE + PropertiesWrapper.get("wps.application.path");

    private static ExecutorService worker;
    private static ProductionService productionService;
    private static Map<String, LocalJob> productionStatusMap;
    private static Logger logger = WpsLogger.getLogger();

    synchronized static ExecutorService getWorker() {
        if (worker == null) {
            logger.log(Level.INFO, "registering GpfProductionService");
            WpsServletContainer.addServletContextListener(new GpfProductionService());
            worker = Executors.newFixedThreadPool(4);
        }
        return worker;
    }

    public synchronized static ProductionService getProductionServiceSingleton() throws IOException, ProductionException {
        if (productionService == null) {
            productionService = createProductionService();
        }
        return productionService;
    }

    private static ProductionService createProductionService() throws IOException, ProductionException {
        LocalWpsProductionServiceFactory productionServiceFactory = new LocalWpsProductionServiceFactory();
        return productionServiceFactory.create(null, getUserAppDataCalWpsDir(), new File(CALWPS_ROOT, STAGING_DIRECTORY));
    }

    public static File getUserAppDataCalWpsDir() {
        String userHome = System.getProperty("user.home");
        return userHome != null ? new File(userHome, ".calwps") : null;
    }

    public synchronized static Map<String, LocalJob> getProductionStatusMap() {
        if (productionStatusMap == null) {
            productionStatusMap = new HashMap<>();
        }
        return productionStatusMap;
    }

    public static String createJobId(String userName) {
        return userName + "-" + new SimpleDateFormat("yyyyMMdd_HHmmssSSS").format(new Date());
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {

    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        synchronized (GpfProductionService.class) {
            if (worker != null) {
                worker.shutdown();
            }
            worker = null;
            productionStatusMap = null;
        }
    }
}
