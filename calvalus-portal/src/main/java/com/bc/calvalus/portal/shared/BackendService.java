package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("backend")
public interface BackendService extends RemoteService {

    PortalProductSet[] getProductSets(String type) throws BackendServiceException;

    PortalProcessor[] getProcessors(String type) throws BackendServiceException;

    PortalProduction[] getProductions(String type) throws BackendServiceException;

    PortalProductionResponse orderProduction(PortalProductionRequest request) throws BackendServiceException;

    WorkStatus getProductionStatus(String productionId) throws BackendServiceException;

    boolean[] cancelProductions(String[] productionIds) throws BackendServiceException;

    boolean[] deleteProductions(String[] productionIds) throws BackendServiceException;

    String stageProductionOutput(String productionId) throws BackendServiceException;
}
