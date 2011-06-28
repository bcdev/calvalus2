package com.bc.calvalus.portal.server;

import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.portal.shared.*;
import com.bc.calvalus.production.*;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * The server side implementation of the RPC processing service.
 * <p/>
 * The actual service object is created by a factory whose implementing class name is given by
 * the servlet initialisation parameter 'calvalus.portal.productionServiceFactory.class'
 * (in context.xml or web.xml).
 *
 * @author Norman
 * @author MarcoZ
 */
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {

    private static final int PRODUCTION_STATUS_OBSERVATION_PERIOD = 2000;

    private ProductionService productionService;
    private BackendConfig backendConfig;
    private Timer statusObserver;

    @Override
    public void init() throws ServletException {
        if (productionService == null) {
            synchronized (this) {
                if (productionService == null) {
                    ServletContext servletContext = getServletContext();
                    initLogger(servletContext);
                    initBackendConfig(servletContext);
                    initProductionService();
                    startObservingProductionService();
                }
            }
        }
    }

    @Override
    public void destroy() {
        if (productionService != null) {
            statusObserver.cancel();
            try {
                productionService.close();
            } catch (IOException e) {
                log("Failed to close production service", e);
            }
            productionService = null;
        }
        super.destroy();
    }

    @Override
    public DtoRegion[] loadRegions(String filter) throws BackendServiceException {
        RegionPersistence regionPersistence = new RegionPersistence(getUserName());
        try {
            return regionPersistence.loadRegions();
        } catch (IOException e) {
            throw new BackendServiceException("Failed to load regions: " + e.getMessage(), e);
        }
    }

    @Override
    public void storeRegions(DtoRegion[] regions) throws BackendServiceException {
        RegionPersistence regionPersistence = new RegionPersistence(getUserName());
        try {
            regionPersistence.storeRegions(regions);
        } catch (IOException e) {
            throw new BackendServiceException("Failed to store regions: " + e.getMessage(), e);
        }
    }

    @Override
    public DtoProductSet[] getProductSets(String filter) throws BackendServiceException {
        try {
            ProductSet[] productSets = productionService.getProductSets(filter);
            DtoProductSet[] dtoProductSets = new DtoProductSet[productSets.length];
            for (int i = 0; i < productSets.length; i++) {
                dtoProductSets[i] = convert(productSets[i]);
            }
            return dtoProductSets;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoProcessorDescriptor[] getProcessors(String filter) throws BackendServiceException {
        try {
            ProcessorDescriptor[] processorDescriptors = productionService.getProcessors(filter);
            DtoProcessorDescriptor[] dtoProcessorDescriptors = new DtoProcessorDescriptor[processorDescriptors.length];
            for (int i = 0; i < processorDescriptors.length; i++) {
                dtoProcessorDescriptors[i] = convert(processorDescriptors[i]);
            }
            return dtoProcessorDescriptors;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoProduction[] getProductions(String filter) throws BackendServiceException {
        try {
            Production[] productions = productionService.getProductions(filter);
            DtoProduction[] dtoProductions = new DtoProduction[productions.length];
            for (int i = 0; i < productions.length; i++) {
                dtoProductions[i] = convert(productions[i]);
            }
            return dtoProductions;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoProductionRequest getProductionRequest(String productionId) throws BackendServiceException {
        try {
            Production production = productionService.getProduction(productionId);
            if (production != null) {
                return convert(production.getProductionRequest());
            } else {
                return null;
            }
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public DtoProductionResponse orderProduction(DtoProductionRequest productionRequest) throws BackendServiceException {
        try {
            ProductionResponse productionResponse = productionService.orderProduction(convert(productionRequest));
            return convert(productionResponse);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public void cancelProductions(String[] productionIds) throws BackendServiceException {
        try {
            productionService.cancelProductions(productionIds);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public void deleteProductions(String[] productionIds) throws BackendServiceException {
        try {
            productionService.deleteProductions(productionIds);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public void stageProductions(String[] productionIds) throws BackendServiceException {
        try {
            productionService.stageProductions(productionIds);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    private DtoProductSet convert(ProductSet productSet) {
        return new DtoProductSet(productSet.getPath(), productSet.getType(), productSet.getName());
    }

    private DtoProcessorDescriptor convert(ProcessorDescriptor processorDescriptor) {
        return new DtoProcessorDescriptor(processorDescriptor.getExecutableName(),
                                          processorDescriptor.getProcessorName(),
                                          processorDescriptor.getDefaultParameter(),
                                          processorDescriptor.getBundleName(),
                                          processorDescriptor.getBundleVersion(),
                                          processorDescriptor.getDescriptionHtml(),
                                          processorDescriptor.getValidMaskExpression(),
                                          convert(processorDescriptor.getOutputVariables()));
    }

    private DtoProcessorVariable[] convert(ProcessorDescriptor.Variable[] outputVariables) {
        int numElems = outputVariables != null ? outputVariables.length : 0;
        DtoProcessorVariable[] processorVariables = new DtoProcessorVariable[numElems];
        for (int i = 0; i < numElems; i++) {
            ProcessorDescriptor.Variable outputVariable = outputVariables[i];
            DtoProcessorVariable dtoProcessorVariable = new DtoProcessorVariable(outputVariable.getName(),
                                                                                 outputVariable.getDefaultAggregator(),
                                                                                 outputVariable.getDefaultWeightCoeff());
            processorVariables[i] = dtoProcessorVariable;
        }
        return processorVariables;
    }

    private DtoProduction convert(Production production) {
        return new DtoProduction(production.getId(),
                                 production.getName(),
                                 production.getProductionRequest().getUserName(),
                                 backendConfig.getStagingPath() + "/" + production.getStagingPath() + "/",
                                 production.isAutoStaging(),
                                 convert(production.getProcessingStatus(), production.getWorkflow()),
                                 convert(production.getStagingStatus()));
    }

    private DtoProductionRequest convert(ProductionRequest productionRequest) {
        return new DtoProductionRequest(productionRequest.getProductionType(),
                                        productionRequest.getParameters());
    }

    private DtoProcessStatus convert(ProcessStatus status, WorkflowItem workflow) {
        Date startTime = workflow.getStartTime();
        Date stopTime = workflow.getStopTime();
        int processingSeconds = 0;
        if (startTime != null) {
            if (stopTime == null) {
                stopTime = new Date();
            }
            processingSeconds = (int) ((stopTime.getTime() - startTime.getTime()) / 1000);
        }
        return new DtoProcessStatus(DtoProcessState.valueOf(status.getState().name()),
                                    status.getMessage(),
                                    status.getProgress(),
                                    processingSeconds);
    }

    private DtoProcessStatus convert(ProcessStatus status) {
        return new DtoProcessStatus(DtoProcessState.valueOf(status.getState().name()),
                                    status.getMessage(),
                                    status.getProgress());
    }

    private DtoProductionResponse convert(ProductionResponse productionResponse) {
        return new DtoProductionResponse(convert(productionResponse.getProduction()));
    }

    private ProductionRequest convert(DtoProductionRequest gwtProductionRequest) {
        String userName = getThreadLocalRequest().getRemoteUser();
        return new ProductionRequest(gwtProductionRequest.getProductionType(),
                                     userName != null ? userName : "calvalus",
                                     gwtProductionRequest.getProductionParameters());
    }

    private BackendServiceException convert(ProductionException e) {
        return new BackendServiceException(e.getMessage(), e);
    }

    private void initLogger(ServletContext servletContext) {
        Logger logger = Logger.getLogger("com.bc.calvalus");
        logger.addHandler(new ServletContextLogHandler(servletContext));
    }

    private void initBackendConfig(ServletContext servletContext) throws ServletException {
        backendConfig = new BackendConfig(servletContext);
        logConfig();
    }

    private void logConfig() {
        log("Calvalus configuration loaded:");
        log("  local context dir          = " + backendConfig.getLocalContextDir());
        log("  local staging dir          = " + backendConfig.getLocalStagingDir());
        log("  local upload dir           = " + backendConfig.getLocalUploadDir());
        log("  staging path               = " + backendConfig.getStagingPath());
        log("  upload path                = " + backendConfig.getUploadPath());
        log("  production service factory = " + backendConfig.getProductionServiceFactoryClassName());
        log("  configurartion:");
        Map<String, String> configMap = backendConfig.getConfigMap();
        Set<Map.Entry<String, String>> entries = configMap.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            log("    " + entry.getKey() + " = " + entry.getValue());
        }
    }

    private void initProductionService() throws ServletException {
        try {
            Class<?> productionServiceFactoryClass = Class.forName(backendConfig.getProductionServiceFactoryClassName());
            ProductionServiceFactory productionServiceFactory = (ProductionServiceFactory) productionServiceFactoryClass.newInstance();
            productionService = productionServiceFactory.create(backendConfig.getConfigMap(),
                                                                backendConfig.getLocalContextDir(),
                                                                backendConfig.getLocalStagingDir());
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void startObservingProductionService() {
        statusObserver = new Timer("StatusObserver", true);
        statusObserver.scheduleAtFixedRate(new TimerTask() {
                                               @Override
                                               public void run() {
                                                   updateProductionStatuses();
                                               }
                                           }, PRODUCTION_STATUS_OBSERVATION_PERIOD, PRODUCTION_STATUS_OBSERVATION_PERIOD);
    }

    private void updateProductionStatuses() {
        final ProductionService productionService = this.productionService;
        if (productionService != null) {
            synchronized (this) {
                productionService.updateStatuses();
            }
        }
    }

    private String getUserName() {
        return getThreadLocalRequest().getUserPrincipal().getName();
    }
}
