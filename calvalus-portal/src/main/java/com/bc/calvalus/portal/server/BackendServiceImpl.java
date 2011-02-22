package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalParameter;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * The server side implementation of the RPC processing service.
 */
@SuppressWarnings("serial")
public class BackendServiceImpl extends RemoteServiceServlet implements BackendService {

    private final BackendService delegate;

    public BackendServiceImpl() {
        super();
        // todo - configure portal so that we can switch between different delegates
        delegate = new DummyBackendService();
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
    public boolean[] deleteProductions(String[] productionIds) throws BackendServiceException {
        return delegate.deleteProductions(productionIds);
    }
}
