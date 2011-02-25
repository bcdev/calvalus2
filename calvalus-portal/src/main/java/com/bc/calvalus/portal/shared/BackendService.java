package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("backend")
public interface BackendService extends RemoteService {
    /**
     * Gets all known product sets.
     *
     * @param filter A filter expression (not yet used).
     * @return The product sets.
     * @throws BackendServiceException If a server error occurred.
     */
    PortalProductSet[] getProductSets(String filter) throws BackendServiceException;

    /**
     * Gets all known processors.
     *
     * @param filter A filter expression (not yet used).
     * @return The processors.
     * @throws BackendServiceException If a server error occurred.
     */
    PortalProcessor[] getProcessors(String filter) throws BackendServiceException;

    /**
     * Gets all known productions.
     *
     * @param filter A filter expression (not yet used).
     * @return The productions.
     * @throws BackendServiceException If a server error occurred.
     */
    PortalProduction[] getProductions(String filter) throws BackendServiceException;

    /**
     * Orders a new productions.
     *
     * @param request The request.
     * @return The response.
     * @throws BackendServiceException If a server error occurred.
     */
    PortalProductionResponse orderProduction(PortalProductionRequest request) throws BackendServiceException;

    void cancelProductions(String[] productionIds) throws BackendServiceException;

    void deleteProductions(String[] productionIds) throws BackendServiceException;
}
