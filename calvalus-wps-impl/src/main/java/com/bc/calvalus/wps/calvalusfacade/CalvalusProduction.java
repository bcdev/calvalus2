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
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles the order production operations (synchronously and asynchronously).
 *
 * @author hans
 */
public class CalvalusProduction {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final int PRODUCTION_STATUS_OBSERVATION_PERIOD = 10000;

    protected LocalProductionStatus orderProductionAsynchronous(ProductionService productionService, ProductionRequest request, String userName)
                throws ProductionException {
        logInfo("Ordering production...");
        logInfo("user : " + userName);
        logInfo("request user name : " + request.getUserName());
        ProductionResponse productionResponse = productionService.orderProduction(request);
        Production production = productionResponse.getProduction();
        logInfo("Production successfully ordered. The production ID is: " + production.getId());

        Timer statusObserver = CalvalusProductionService.getStatusObserverSingleton();
        synchronized (CalvalusProductionService.getUserProductionMap()) {
            if (!CalvalusProductionService.getUserProductionMap().containsKey(userName)) {
                CalvalusProductionService.getUserProductionMap().put(userName, 1);
                statusObserver.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            updateProductionStatuses(userName);
                        } catch (IOException | ProductionException e) {
                            LOG.log(Level.SEVERE, "Unable to update production status.", e);
                        }
                    }
                }, PRODUCTION_STATUS_OBSERVATION_PERIOD, PRODUCTION_STATUS_OBSERVATION_PERIOD);
            }
        }

        ProcessStatus status = production.getProcessingStatus();
        return new LocalProductionStatus(production.getId(),
                                         status.getState(),
                                         status.getProgress(),
                                         status.getMessage(),
                                         null);

//        return production.getId();
    }

    protected LocalProductionStatus orderProductionSynchronous(ProductionService productionService, ProductionRequest request)
                throws ProductionException, InterruptedException {
        logInfo("Ordering production...");
        ProductionResponse productionResponse = productionService.orderProduction(request);
        Production production = productionResponse.getProduction();
        logInfo("Production successfully ordered. The production ID is: " + production.getId());
        observeProduction(productionService, production);
        ProcessStatus status = production.getProcessingStatus();
        return new LocalProductionStatus(production.getId(),
                                         status.getState(),
                                         status.getProgress(),
                                         status.getMessage(),
                                         null);
//        return production.getId();
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

    private void updateProductionStatuses(String userName) throws IOException, ProductionException {
        final ProductionService productionService = CalvalusProductionService.getProductionServiceSingleton();
        if (productionService != null) {
            synchronized (this) {
                try {
                    productionService.updateStatuses(userName);
                } catch (IllegalStateException exception) {
                    System.out.println("Trying to stop thread " + Thread.currentThread().getName());
                    Timer statusObserver = CalvalusProductionService.getStatusObserverSingleton();
                    statusObserver.cancel();
                }
            }
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
