package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.PortalProductionStatus;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * The server side implementation of the RPC processing service.
 * <p/>
 * The actual service is implemented by a class given by
 * the servlet initialisation parameter 'calvalus.portal.backendService.class'
 * (in context.xml or web.xml). Its value must be the name of a class that
 * implements the {@link BackendService} interface and has a one-argument constructor
 * taking the current {@link javax.servlet.ServletContext}.
 */
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {

    private BackendService delegate;

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        initService();
        super.service(req, res);
    }

    @Override
    public PortalProductSet[] getProductSets(String filter) throws BackendServiceException {
        return delegate.getProductSets(filter);
    }

    @Override
    public PortalProcessor[] getProcessors(String filter) throws BackendServiceException {
        return delegate.getProcessors(filter);
    }

    @Override
    public PortalProduction[] getProductions(String filter) throws BackendServiceException {
        return delegate.getProductions(filter);
    }

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {
        return delegate.orderProduction(productionRequest);
    }

    @Override
    public boolean[] cancelProductions(String[] productionIds) throws BackendServiceException {
        return delegate.cancelProductions(productionIds);
    }

    @Override
    public boolean[] deleteProductions(String[] productionIds) throws BackendServiceException {
        return delegate.deleteProductions(productionIds);
    }

    @Override
    public String stageProductionOutput(String productionId) throws BackendServiceException {
        return delegate.stageProductionOutput(productionId);
    }

    private void initService() throws ServletException {

        if (delegate == null) {
            String className = getServletContext().getInitParameter("calvalus.portal.backendService.class");
            if (className != null) {
                try {
                    delegate = (BackendService) Class.forName(className).getConstructor(ServletContext.class).newInstance(getServletContext());
                } catch (Exception e) {
                    throw new ServletException(e);
                }
            } else {
                throw new ServletException(String.format("Missing servlet initialisation parameter '%s'",
                                                         "calvalus.portal.backendService.class"));
            }
            // Make the service available to other servlets.
            getServletContext().setAttribute("calvalus.portal.backendService", delegate);
        }
    }
}
