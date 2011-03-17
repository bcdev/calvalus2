package com.bc.calvalus.portal.server;

import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.GsProcessState;
import com.bc.calvalus.portal.shared.GsProcessStatus;
import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.bc.calvalus.portal.shared.GsProductSet;
import com.bc.calvalus.portal.shared.GsProduction;
import com.bc.calvalus.portal.shared.GsProductionRequest;
import com.bc.calvalus.portal.shared.GsProductionResponse;
import com.bc.calvalus.production.ProcessorDescriptor;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

/**
 * The server side implementation of the RPC processing service.
 * <p/>
 * The actual service is implemented by a class given by
 * the servlet initialisation parameter 'calvalus.portal.productionServiceFactory.class'
 * (in context.xml or web.xml). Its value must be the name of a class that
 * implements the {@link BackendService} interface and has a one-argument constructor
 * taking the current {@link javax.servlet.ServletContext}.
 *
 * @author Norman
 * @author MarcoZ
 */
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {

    private static final String PRODUCTION_SERVICE_FACTORY_CLASS = "calvalus.portal.productionServiceFactory.class";
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
                    initProductionService(servletContext);
                    startObservingProductionService();
                }
            }
        }
    }

    @Override
    public void destroy() {
        if (productionService != null) {
            statusObserver.cancel();
            productionService = null;
        }
        super.destroy();
    }

    @Override
    public GsProductSet[] getProductSets(String filter) throws BackendServiceException {
        try {
            ProductSet[] productSets = productionService.getProductSets(filter);
            GsProductSet[] gsProductSets = new GsProductSet[productSets.length];
            for (int i = 0; i < productSets.length; i++) {
                gsProductSets[i] = convert(productSets[i]);
            }
            return gsProductSets;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public GsProcessorDescriptor[] getProcessors(String filter) throws BackendServiceException {
        try {
            ProcessorDescriptor[] processorDescriptors = productionService.getProcessors(filter);
            GsProcessorDescriptor[] gsProcessorDescriptors = new GsProcessorDescriptor[processorDescriptors.length];
            for (int i = 0; i < processorDescriptors.length; i++) {
                gsProcessorDescriptors[i] = convert(processorDescriptors[i]);
            }
            return gsProcessorDescriptors;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public GsProduction[] getProductions(String filter) throws BackendServiceException {
        try {
            Production[] productions = productionService.getProductions(filter);
            GsProduction[] gsProductions = new GsProduction[productions.length];
            for (int i = 0; i < productions.length; i++) {
                gsProductions[i] = convert(productions[i]);
            }
            return gsProductions;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public GsProductionResponse orderProduction(GsProductionRequest productionRequest) throws BackendServiceException {
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

    private GsProductSet convert(ProductSet productSet) {
        return new GsProductSet(productSet.getId(), productSet.getType(), productSet.getName());
    }

    private GsProcessorDescriptor convert(ProcessorDescriptor processorDescriptor) {
        return new GsProcessorDescriptor(processorDescriptor.getExecutableName(), processorDescriptor.getProcessorName(),
                                         processorDescriptor.getDefaultParameters(), processorDescriptor.getBundleName(),
                                         processorDescriptor.getBundleVersions());
    }

    private GsProduction convert(Production production) {
        return new GsProduction(production.getId(),
                                production.getName(),
                                production.getUser(),
                                backendConfig.getStagingPath() + "/" + production.getStagingPath(),
                                production.isAutoStaging(),
                                convert(production.getProcessingStatus()),
                                convert(production.getStagingStatus()));
    }

    private GsProcessStatus convert(ProcessStatus status) {
        return new GsProcessStatus(GsProcessState.valueOf(status.getState().name()),
                                   status.getMessage(),
                                   status.getProgress());
    }

    private GsProductionResponse convert(ProductionResponse productionResponse) {
        return new GsProductionResponse(convert(productionResponse.getProduction()));
    }

    private ProductionRequest convert(GsProductionRequest gwtProductionRequest) {
        return new ProductionRequest(gwtProductionRequest.getProductionType(),
                                     gwtProductionRequest.getUserName(),
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
    }

    private void initProductionService(ServletContext servletContext) throws ServletException {
        String className = servletContext.getInitParameter(PRODUCTION_SERVICE_FACTORY_CLASS);
        if (className == null) {
            throw new ServletException(String.format("Missing servlet initialisation parameter '%s'",
                                                     PRODUCTION_SERVICE_FACTORY_CLASS));
        }
        try {
            Class<?> productionServiceFactoryClass = Class.forName(className);
            ProductionServiceFactory productionServiceFactory = (ProductionServiceFactory) productionServiceFactoryClass.newInstance();
            productionService = productionServiceFactory.create(getServiceConfiguration(servletContext),
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

    private static Map<String, String> getServiceConfiguration(ServletContext servletContext) {
        Map<String, String> map = new HashMap<String, String>();
        Enumeration elements = servletContext.getInitParameterNames();
        while (elements.hasMoreElements()) {
            String name = (String) elements.nextElement();
            String value = servletContext.getInitParameter(name);
            map.put(name, value);
        }
        return map;
    }

    private void updateProductionStatuses() {
        final ProductionService productionService = this.productionService;
        if (productionService != null) {
            synchronized (this) {
                productionService.updateStatuses();
            }
        }
    }

}
