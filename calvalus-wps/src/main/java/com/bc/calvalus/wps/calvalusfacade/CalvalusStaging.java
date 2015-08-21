package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by hans on 10/08/2015.
 */
public class CalvalusStaging {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public void stageProduction(ProductionService productionService, Production production)
                throws ProductionException, InterruptedException {
        logInfo("Staging results...");
        productionService.stageProductions(production.getId());
        observeStagingStatus(productionService, production);
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
