package com.bc.calvalus.portal.server;

import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.PortalProductionState;
import com.bc.calvalus.portal.shared.PortalProductionStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionProcessor;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.bc.calvalus.production.ProductionStatus;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
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

    private static final String CALVALUS_PORTAL_PRODUCTION_SERVICE_FACTORY_CLASS = "calvalus.portal.productionServiceFactory.class";
    private ProductionService productionService;

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        initDelegate();
        super.service(req, res);
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        initDelegate();
        super.service(req, resp);
    }

    @Override
    public PortalProductSet[] getProductSets(String filter) throws BackendServiceException {
        try {
            ProductSet[] productSets = productionService.getProductSets(filter);
            PortalProductSet[] portalProductSets = new PortalProductSet[productSets.length];
            for (int i = 0; i < productSets.length; i++) {
                portalProductSets[i] = convert(productSets[i]);
            }
            return portalProductSets;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public PortalProcessor[] getProcessors(String filter) throws BackendServiceException {
        try {
            ProductionProcessor[] processors = productionService.getProcessors(filter);
            PortalProcessor[] portalProcessors = new PortalProcessor[processors.length];
            for (int i = 0; i < processors.length; i++) {
                portalProcessors[i] = convert(processors[i]);
            }
            return portalProcessors;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public PortalProduction[] getProductions(String filter) throws BackendServiceException {
        try {
            Production[] productions = productionService.getProductions(filter);
            PortalProduction[] portalProductions = new PortalProduction[productions.length];
            for (int i = 0; i < productions.length; i++) {
                portalProductions[i] = convert(productions[i]);
            }
            return portalProductions;
        } catch (ProductionException e) {
            throw convert(e);
        }
    }

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {
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

    private PortalProductSet convert(ProductSet productSet) {
        return new PortalProductSet(productSet.getId(), productSet.getType(), productSet.getName());
    }

    private PortalProcessor convert(ProductionProcessor processor) {
        return new PortalProcessor(processor.getExecutableName(), processor.getProcessorName(),
                                   processor.getDefaultParameters(), processor.getBundleName(),
                                   processor.getBundleVersions());
    }

    private PortalProduction convert(Production production) {
        return new PortalProduction(production.getId(),
                                    production.getName(),
                                    production.getOutputUrl(),
                                    convert(production.getProcessingStatus()),
                                    convert(production.getStagingStatus()));
    }

    private PortalProductionStatus convert(ProductionStatus status) {
        return new PortalProductionStatus(PortalProductionState.valueOf(status.getState().name()),
                                          status.getMessage(),
                                          status.getProgress());
    }

    private PortalProductionResponse convert(ProductionResponse productionResponse) {
        return new PortalProductionResponse(convert(productionResponse.getProduction()));
    }

    private ProductionRequest convert(PortalProductionRequest portalProductionRequest) {
        return new ProductionRequest(portalProductionRequest.getProductionType(), portalProductionRequest.getProductionParameters());
    }

    private BackendServiceException convert(ProductionException e) {
        return new BackendServiceException(e.getMessage(), e);
    }

    private void initDelegate() throws ServletException {
        if (productionService == null) {
            synchronized (this) {
                if (productionService == null) {
                    ServletContext servletContext = getServletContext();
                    String className = servletContext.getInitParameter(CALVALUS_PORTAL_PRODUCTION_SERVICE_FACTORY_CLASS);
                    if (className != null) {
                        initLogger(servletContext);
                        Map<String, String> serviceConfiguration = getServiceConfiguration(servletContext);
                        try {
                            Class<?> productionServiceFactoryClass = Class.forName(className);
                            ProductionServiceFactory productionServiceFactory = (ProductionServiceFactory) productionServiceFactoryClass.newInstance();
                            PortalConfig portalConfig = new PortalConfig(servletContext);
                            productionService = productionServiceFactory.create(serviceConfiguration,
                                                                                portalConfig.getStagingPath(),
                                                                                portalConfig.getLocalStagingDir().getPath());
                        } catch (Exception e) {
                            throw new ServletException(e);
                        }
                    } else {
                        throw new ServletException(String.format("Missing servlet initialisation parameter '%s'",
                                                                 CALVALUS_PORTAL_PRODUCTION_SERVICE_FACTORY_CLASS));
                    }
                }
            }
        }
    }

    private void initLogger(ServletContext servletContext) {
        Logger logger = Logger.getLogger("com.bc.calvalus");
        logger.addHandler(new ServletContextLogHandler(servletContext));
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

}
