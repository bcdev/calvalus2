package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.ColorPaletteSet;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import com.bc.wps.api.exceptions.MissingParameterValueException;
import com.bc.wps.api.schema.Execute;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles the order production operations (synchronously and asynchronously).
 *
 * @author hans
 */
class CalvalusProduction {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final int PRODUCTION_STATUS_OBSERVATION_PERIOD = 10000;

    LocalProductionStatus orderProductionAsynchronous(Execute executeRequest, String userName, CalvalusFacade calvalusFacade) throws WpsProductionException {
        try {
            ServiceContainer serviceContainer = CalvalusProductionService.getServiceContainerSingleton();
            ProductionRequest request = createProductionRequest(executeRequest, userName, serviceContainer, calvalusFacade);
            return doProductionAsynchronous(request, serviceContainer.getProductionService(), userName);
        } catch (ProductionException | IOException | InvalidParameterValueException | WpsProcessorNotFoundException |
                    MissingParameterValueException | InvalidProcessorIdException | JAXBException exception) {
            throw new WpsProductionException("Processing failed : " + exception.getMessage(), exception);
        }
    }

    LocalProductionStatus orderProductionSynchronous(Execute executeRequest, String userName, CalvalusFacade calvalusFacade) throws WpsProductionException {
        try {
            ServiceContainer serviceContainer = CalvalusProductionService.getServiceContainerSingleton();
            ProductionRequest request = createProductionRequest(executeRequest, userName, serviceContainer, calvalusFacade);
            LocalProductionStatus status = doProductionSynchronous(serviceContainer.getProductionService(), request);
            String jobId = status.getJobId();
            calvalusFacade.stageProduction(jobId);
            calvalusFacade.observeStagingStatus(jobId);
            status.setResultUrls(calvalusFacade.getProductResultUrls(jobId));
            status.setStopDate(new Date());
            status.setState(ProcessState.COMPLETED);
            calvalusFacade.generateProductMetadata(jobId);
            return status;
        } catch (WpsResultProductException | JAXBException | MissingParameterValueException | InvalidProcessorIdException | WpsStagingException |
                    ProductMetadataException | InterruptedException | WpsProcessorNotFoundException | ProductionException |
                    InvalidParameterValueException | IOException exception) {
            throw new WpsProductionException("Processing failed : " + exception.getMessage(), exception);
        }
    }

    private LocalProductionStatus doProductionSynchronous(ProductionService productionService, ProductionRequest request)
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
    }

    private LocalProductionStatus doProductionAsynchronous(ProductionRequest request, ProductionService productionService, String userName)
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

    }

    private ProductionRequest createProductionRequest(Execute executeRequest, String userName,
                                                      ServiceContainer serviceContainer, CalvalusFacade calvalusFacade)
                throws MissingParameterValueException, InvalidParameterValueException, JAXBException,
                       InvalidProcessorIdException, WpsProcessorNotFoundException, IOException, ProductionException {
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);
        String processorId = executeRequest.getIdentifier().getValue();
        ProcessorNameConverter parser = new ProcessorNameConverter(processorId);
        WpsProcess calvalusProcessor = calvalusFacade.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, calvalusProcessor,
                                                                       getProductSets(userName, serviceContainer),
                                                                       calvalusFacade.getRequestHeaderMap());
        return new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                     userName,
                                     calvalusDataInputs.getInputMapFormatted());
    }

    private ProductSet[] getProductSets(String userName, ServiceContainer serviceContainer) throws ProductionException, IOException {
        List<ProductSet> productSets = new ArrayList<>();
        productSets.addAll(Arrays.asList(serviceContainer.getInventoryService().getProductSets(userName, "")));
        productSets.addAll(Arrays.asList(serviceContainer.getInventoryService().getProductSets(userName, "user=" + userName)));
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

    private ColorPaletteSet[] getColorPaletteSets(String userName, ServiceContainer serviceContainer) throws ProductionException, IOException {
        List<ColorPaletteSet> colorPaletteSets = new ArrayList<>();
        colorPaletteSets.addAll(Arrays.asList(serviceContainer.getColorPaletteService().getColorPaletteSets(userName, "")));
        colorPaletteSets.addAll(Arrays.asList(serviceContainer.getColorPaletteService().getColorPaletteSets(userName, "user=" + userName)));
        return colorPaletteSets.toArray(new ColorPaletteSet[colorPaletteSets.size()]);
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
        final ProductionService productionService = CalvalusProductionService.getServiceContainerSingleton().getProductionService();
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
        return new Thread(() -> {
            try {
                workflow.kill();
            } catch (Exception e) {
                logError("Failed to shutdown production: " + e.getMessage());
            }
        });
    }

    private void logError(String errorMessage) {
        LOG.log(Level.SEVERE, errorMessage);
    }

    private void logInfo(String message) {
        LOG.info(message);
    }
}
