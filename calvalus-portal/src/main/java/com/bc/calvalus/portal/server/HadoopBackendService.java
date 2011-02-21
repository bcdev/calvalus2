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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An BackendService implementation that delegates to a Hadoop cluster.
 */
public class HadoopBackendService implements BackendService {

    @Override
    public PortalProductSet[] getProductSets(String type) throws BackendServiceException {
        // todo - implement me
        throw new BackendServiceException("Method 'getProductSets' not implemented");
    }

    @Override
    public PortalProcessor[] getProcessors(String type) throws BackendServiceException {
        // todo - implement me
        throw new BackendServiceException("Method 'getProcessors' not implemented");
    }

    @Override
    public PortalProduction[] getProductions(String type) throws BackendServiceException {
        // todo - implement me
        throw new BackendServiceException("Method 'getProductions' not implemented");
    }

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {
        // todo - implement me
        throw new BackendServiceException("Method 'orderProduction' not implemented");
    }

    @Override
    public WorkStatus getProductionStatus(String productionId) throws BackendServiceException {
        // todo - implement me
        throw new BackendServiceException("Method 'getProductionStatus' not implemented");
    }
}
