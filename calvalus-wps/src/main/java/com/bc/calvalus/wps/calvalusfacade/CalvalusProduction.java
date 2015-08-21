package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by hans on 11/08/2015.
 */
public class CalvalusProduction {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public Production orderProduction(ProductionService productionService, ProductionRequest request)
                throws ProductionException, InterruptedException {
        logInfo("Ordering production...");
        ProductionResponse productionResponse = productionService.orderProduction(request);
        Production production = productionResponse.getProduction();
        logInfo("Production successfully ordered. The production ID is: " + production.getId());
        observeProduction(productionService, production);
        return production;
    }

    private void observeProduction(ProductionService productionService, Production production) throws InterruptedException {
        final Thread shutDownHook = createShutdownHook(production.getWorkflow());
        Runtime.getRuntime().addShutdownHook(shutDownHook);

        String userName = production.getProductionRequest().getUserName();
        while (!production.getProcessingStatus().getState().isDone()) {
            Thread.sleep(5000);
            productionService.updateStatuses(userName);
            ProcessStatus processingStatus = production.getProcessingStatus();
            logInfo(String.format("Production remote status: state=%s, progress=%s, message='%s'",
                                  processingStatus.getState(),
                                  processingStatus.getProgress(),
                                  processingStatus.getMessage()));
        }
        Runtime.getRuntime().removeShutdownHook(shutDownHook);

        if (production.getProcessingStatus().getState() == ProcessState.COMPLETED) {
            logInfo("Production completed. Output directory is " + production.getStagingPath());
        } else {
            logError("Error: Production did not complete normally: " + production.getProcessingStatus().getMessage());
        }
    }

    private Thread createShutdownHook(final WorkflowItem workflow) {
        return new Thread() {
            @Override
            public void run() {
                try {
                    workflow.kill();
                } catch (Exception e) {
                    logError("Failed to shutdown production: " + e.getMessage());
                }
            }
        };
    }

    private void logError(String errorMessage) {
        LOG.log(Level.SEVERE, errorMessage);
    }

    private void logInfo(String message) {
        LOG.info(message);
    }
}
