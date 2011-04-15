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
    GsProductSet[] getProductSets(String filter) throws BackendServiceException;

    /**
     * Gets all known processors.
     *
     * @param filter A filter expression (not yet used).
     * @return The processors.
     * @throws BackendServiceException If a server error occurred.
     */
    GsProcessorDescriptor[] getProcessors(String filter) throws BackendServiceException;

    /**
     * Gets all known productions.
     *
     * @param filter A filter expression (not yet used).
     * @return The productions.
     * @throws BackendServiceException If a server error occurred.
     */
    GsProduction[] getProductions(String filter) throws BackendServiceException;

    /**
     * Orders a new productions.
     *
     * @param request The request.
     * @return The response.
     * @throws BackendServiceException If a server error occurred.
     */
    GsProductionResponse orderProduction(GsProductionRequest request) throws BackendServiceException;

    /**
     * Gets the production request for the given production ID.
     *
     * @param productionId The production ID
     * @return The production request, or {@code null} if none with the given ID was found.
     * @throws BackendServiceException If a server error occurred.
     */
    GsProductionRequest getProductionRequest(String productionId) throws BackendServiceException;

    void cancelProductions(String[] productionIds) throws BackendServiceException;

    void deleteProductions(String[] productionIds) throws BackendServiceException;

    void stageProductions(String[] productionIds) throws BackendServiceException;
}
