package com.bc.calvalus.wps.calvalusfacade;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.wps.api.WpsServerContext;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles all the product-staging-related operations.
 *
 * @author hans
 */
public class CalvalusStaging {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String CALWPS_ROOT_PATH = "/webapps/calwps/";
    private static final String APP_NAME = "calwps";

    private final WpsServerContext wpsServerContext;

    public CalvalusStaging(WpsServerContext wpsServerContext) {
        this.wpsServerContext = wpsServerContext;
    }

    protected void stageProduction(ProductionService productionService, Production production) throws ProductionException {
        logInfo("Staging results...");
        productionService.stageProductions(production.getId());
    }

    protected List<String> getProductResultUrls(Map<String, String> calvalusDefaultConfig, Production production) {
        String stagingDirectoryPath = calvalusDefaultConfig.get("calvalus.wps.staging.path") + "/" + production.getStagingPath();
        File stagingDirectory = new File((System.getProperty("catalina.base") + CALWPS_ROOT_PATH) + stagingDirectoryPath);
        File[] productResultFiles = stagingDirectory.listFiles();
        List<String> productResultUrls = new ArrayList<>();
        if (productResultFiles != null) {
            for (File productResultFile : productResultFiles) {
                String productUrl = "http://"
                                    + wpsServerContext.getHostAddress()
                                    + ":" + wpsServerContext.getPort()
                                    + "/" + APP_NAME
                                    + "/" + stagingDirectoryPath
                                    + "/" + productResultFile.getName();
                productResultUrls.add(productUrl);
            }
        }
        return productResultUrls;
    }

    protected void observeStagingStatus(ProductionService productionService, Production production)
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
