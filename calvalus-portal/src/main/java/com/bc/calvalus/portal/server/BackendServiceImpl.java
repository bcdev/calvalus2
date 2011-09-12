package com.bc.calvalus.portal.server;

import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.DtoProcessState;
import com.bc.calvalus.portal.shared.DtoProcessStatus;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoProduction;
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.bc.calvalus.portal.shared.DtoProductionResponse;
import com.bc.calvalus.portal.shared.DtoRegion;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
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
            } catch (Exception e) {
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
            List<DtoProcessorDescriptor> dtoProcessorDescriptors = new ArrayList<DtoProcessorDescriptor>();
            final BundleDescriptor[] bundleDescriptors = productionService.getBundles(filter);
            for (BundleDescriptor bundleDescriptor : bundleDescriptors) {
                DtoProcessorDescriptor[] dtoDescriptors = getDtoProcessorDescriptors(bundleDescriptor);
                dtoProcessorDescriptors.addAll(Arrays.asList(dtoDescriptors));
            }
            return dtoProcessorDescriptors.toArray(new DtoProcessorDescriptor[dtoProcessorDescriptors.size()]);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    private DtoProcessorDescriptor[] getDtoProcessorDescriptors(BundleDescriptor bundleDescriptor) {
        ProcessorDescriptor[] processorDescriptors = bundleDescriptor.getProcessorDescriptors();
        DtoProcessorDescriptor[] dtoDescriptors = new DtoProcessorDescriptor[processorDescriptors.length];
        for (int i = 0; i < processorDescriptors.length; i++) {
            dtoDescriptors[i] = convert(bundleDescriptor.getBundleName(), bundleDescriptor.getBundleVersion(),
                                        processorDescriptors[i]);
        }
        return dtoDescriptors;
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

    @Override
    public String[] listUserFiles(String dirPath) throws BackendServiceException {
        try {
            return productionService.listUserFiles(getUserName(), dirPath);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public boolean removeUserFile(String filePath) throws BackendServiceException {
        try {
            return productionService.removeUserFile(getUserName(), filePath);
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    private DtoProductSet convert(ProductSet productSet) {
        return new DtoProductSet(productSet.getName(),
                                 productSet.getPath(),
                                 productSet.getMinDate(),
                                 productSet.getMaxDate());
    }

    private DtoProcessorDescriptor convert(String bundleName, String bundleVersion,
                                           ProcessorDescriptor processorDescriptor) {
        return new DtoProcessorDescriptor(processorDescriptor.getExecutableName(),
                                          processorDescriptor.getProcessorName(),
                                          processorDescriptor.getDefaultParameters() != null ? processorDescriptor.getDefaultParameters().trim() : "",
                                          bundleName,
                                          bundleVersion,
                                          processorDescriptor.getDescriptionHtml() != null ? processorDescriptor.getDescriptionHtml() : "",
                                          processorDescriptor.getMaskExpression(),
                                          convert(processorDescriptor.getOutputVariables()));
    }

    private DtoProcessorVariable[] convert(ProcessorDescriptor.Variable[] outputVariables) {
        if (outputVariables == null) {
            return new DtoProcessorVariable[0];
        }
        DtoProcessorVariable[] processorVariables = new DtoProcessorVariable[outputVariables.length];
        for (int i = 0; i < outputVariables.length; i++) {
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
        return new ProductionRequest(gwtProductionRequest.getProductionType(),
                                     getUserName(),
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
        log("  configuration:");
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
                                                                backendConfig.getLocalAppDataDir(),
                                                                backendConfig.getLocalStagingDir());
            // Make the production servlet accessible by other servlets:
            getServletContext().setAttribute("productionService", productionService);
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
        return getUserName(getThreadLocalRequest());
    }

    public static String getUserName(HttpServletRequest request) {
        Principal userPrincipal = request.getUserPrincipal();
        if (userPrincipal != null) {
            return userPrincipal.getName();
        }
        String userName = request.getRemoteUser();
        if (userName != null) {
            return userName;
        }
        return "anonymous";
    }
}
