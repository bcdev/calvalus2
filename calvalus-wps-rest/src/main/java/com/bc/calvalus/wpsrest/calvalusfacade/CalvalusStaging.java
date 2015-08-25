package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by hans on 10/08/2015.
 */
public class CalvalusStaging {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String WEBAPPS_ROOT = "/webapps/ROOT/";
    private static final String PORT_NUMBER = "9080";

    public void stageProduction(ProductionService productionService, Production production)
                throws ProductionException, InterruptedException {
        logInfo("Staging results...");
        productionService.stageProductions(production.getId());
        observeStagingStatus(productionService, production);
    }

    public List<String> getProductResultUrls(CalvalusConfig calvalusConfig, Production production) throws UnknownHostException {
        String stagingDirectoryPath = calvalusConfig.getDefaultConfig().get("calvalus.wps.staging.path") + "/" + production.getStagingPath();
        File stagingDirectory = new File((System.getProperty("catalina.base") + WEBAPPS_ROOT) + stagingDirectoryPath);
        File[] productResultFles = stagingDirectory.listFiles();
        List<String> productResultUrls = new ArrayList<>();
        if (productResultFles != null) {
            for (File productResultFile : productResultFles) {
                String productUrl = "http://"
                                    + InetAddress.getLocalHost().getHostName()
                                    + ":" + PORT_NUMBER
                                    + "/" + stagingDirectoryPath
                                    + "/" + productResultFile.getName();
                productResultUrls.add(productUrl);
            }
        }
        return productResultUrls;
    }

    private void observeStagingStatus(ProductionService productionService, Production production)
                throws InterruptedException {
        String userName = production.getProductionRequest().getUserName();
        while (!production.getStagingStatus().isDone()) {
            Thread.sleep(500);
            productionService.updateStatuses(userName);
            ProcessStatus stagingStatus = production.getStagingStatus();
            logInfo(String.format("Staging status: state=%s, progress=%s, message='%s'",
                                  stagingStatus.getState(),
                                  stagingStatus.getProgress(),
                                  stagingStatus.getMessage()));
        }
        if (production.getStagingStatus().getState() == ProcessState.COMPLETED) {
            logInfo("Staging completed.");
        } else {
            logError("Error: Staging did not complete normally: " + production.getStagingStatus().getMessage());
        }
    }

    private void logError(String errorMessage) {
        LOG.log(Level.SEVERE, errorMessage);
    }

    private void logInfo(String message) {
        LOG.info(message);
    }

}
