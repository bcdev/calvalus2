package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.wps.exceptions.SqlStoreException;
import com.bc.wps.utilities.WpsLogger;
import com.bc.wps.utilities.WpsServletContainer;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.File;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class GpfProductionService implements ServletContextListener {

    private static ExecutorService worker;
    private static LocalProductionService productionService;
    private static Logger logger = WpsLogger.getLogger();

    synchronized static ExecutorService getWorker() {
        if (worker == null) {
            logger.log(Level.INFO, "registering GpfProductionService");
            WpsServletContainer.addServletContextListener(new GpfProductionService());
            worker = Executors.newFixedThreadPool(4);
        }
        return worker;
    }

    public synchronized static LocalProductionService getProductionServiceSingleton() throws SqlStoreException {
        if (productionService == null) {
            productionService = createProductionService();
        }
        return productionService;
    }

    static String createJobId(String userName) {
        return userName + "-" + DateUtils.createDateFormat("yyyyMMdd_HHmmssSSS").format(new Date());
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
            try {
                productionService.close();
            } catch (SqlStoreException exception) {
                logger.log(Level.SEVERE, "Unable to close SQL connection", exception);
            }
        }
    }

    private static File getUserAppDataCalWpsDir() {
        String userHome = System.getProperty("user.home");
        return userHome != null ? new File(userHome, ".calwps") : null;
    }

    private static LocalProductionService createProductionService() throws SqlStoreException {
        LocalWpsProductionServiceFactory productionServiceFactory = new LocalWpsProductionServiceFactory();
        return productionServiceFactory.create(getUserAppDataCalWpsDir());
    }
}
