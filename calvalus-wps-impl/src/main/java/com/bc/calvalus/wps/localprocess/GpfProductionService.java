package com.bc.calvalus.wps.localprocess;

import com.bc.wps.utilities.PropertiesWrapper;
import com.bc.wps.utilities.WpsLogger;
import com.bc.wps.utilities.WpsServletContainer;
import com.sun.jersey.api.uri.UriBuilderImpl;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class GpfProductionService implements ServletContextListener {

    private static ExecutorService worker;

    private static Map<String, LocalProductionStatus> productionStatusMap;

    private static Logger logger = WpsLogger.getLogger();

    synchronized static ExecutorService getWorker() {
        if (worker == null) {
            logger.log(Level.INFO, "registering GpfProductionService");
            WpsServletContainer.addServletContextListener(new GpfProductionService());
            worker = Executors.newFixedThreadPool(4);
        }
        return worker;
    }

    public synchronized static Map<String, LocalProductionStatus> getProductionStatusMap() {
        if (productionStatusMap == null) {
            productionStatusMap = new HashMap<>();
        }
        return productionStatusMap;
    }

    public static String createJobId(String userName) {
        return userName + "-" + new SimpleDateFormat("yyyyMMdd_HHmmssSSS").format(new Date());
    }

    static List<String> getProductUrls(String hostAddress, int portNumber, File targetDir) {
        List<String> resultUrls = new ArrayList<>();
        String[] resultProductNames = targetDir.list();
        if (resultProductNames != null) {
            for (String filename : resultProductNames) {
                UriBuilder builder = new UriBuilderImpl();
                String productUrl = builder.scheme("http")
                            .host(hostAddress)
                            .port(portNumber)
                            .path(PropertiesWrapper.get("wps.application.name"))
                            .path(PropertiesWrapper.get("utep.output.directory"))
                            .path(targetDir.getParentFile().getName())
                            .path(targetDir.getName())
                            .path(filename).build().toString();
                resultUrls.add(productUrl);
            }
        }
        return resultUrls;
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
