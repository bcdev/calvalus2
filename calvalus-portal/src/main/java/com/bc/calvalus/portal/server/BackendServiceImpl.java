package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * The server side implementation of the RPC processing service.
 */
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {

    private BackendService delegate;

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        initDelegate();
        super.service(req, res);
    }

    @Override
    public PortalProductSet[] getProductSets(String type) throws BackendServiceException {
        return delegate.getProductSets(type);
    }

    @Override
    public PortalProcessor[] getProcessors(String type) throws BackendServiceException {
        return delegate.getProcessors(type);
    }

    @Override
    public PortalProduction[] getProductions(String type) throws BackendServiceException {
        return delegate.getProductions(type);
    }

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {
        return delegate.orderProduction(productionRequest);
    }

    @Override
    public WorkStatus getProductionStatus(String productionId) throws BackendServiceException {
        return delegate.getProductionStatus(productionId);
    }

    @Override
    public boolean[] cancelProductions(String[] productionIds) throws BackendServiceException {
        return delegate.cancelProductions(productionIds);
    }

    @Override
    public boolean[] deleteProductions(String[] productionIds) throws BackendServiceException {
        return delegate.deleteProductions(productionIds);
    }

    private void initDelegate() throws ServletException {
        if (delegate == null) {
            String className = getServletContext().getInitParameter("calvalusPortal.backendService.class");
            if (className != null) {
                try {
                    delegate = (BackendService) Class.forName(className).newInstance();
                } catch (Exception e) {
                    delegate = new DummyBackendService();
                    throw new ServletException(e);
                }
            } else {
                delegate = new DummyBackendService();
            }
            getServletContext().setAttribute("calvalusPortal.backendService", delegate);
        }
    }
}
