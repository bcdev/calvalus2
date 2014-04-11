package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("backend")
public interface BackendService extends RemoteService {

    String PARAM_NAME_CURRENT_USER_ONLY = "currentUserOnly";


    /**
     * Gets all known regions.
     *
     * @param filter A filter expression (not yet used).
     *
     * @return The array of regions.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoRegion[] loadRegions(String filter) throws BackendServiceException;

    /**
     * Persists the provided regions.
     *
     * @param regions The regions to persist.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    void storeRegions(DtoRegion[] regions) throws BackendServiceException;

    /**
     * Gets all known product sets.
     *
     * @param filter A filter expression (not yet used).
     *
     * @return The product sets.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoProductSet[] getProductSets(String filter) throws BackendServiceException;

    /**
     * Gets all known processors.
     *
     * @param filter A filter expression (not yet used).
     *
     * @return The processors.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoProcessorDescriptor[] getProcessors(String filter) throws BackendServiceException;

    /**
     * Gets all known productions.
     *
     * @param filter A filter expression (not yet used).
     *
     * @return The productions.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoProduction[] getProductions(String filter) throws BackendServiceException;

    /**
     * Orders a new productions.
     *
     * @param request The request.
     *
     * @return The response.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoProductionResponse orderProduction(DtoProductionRequest request) throws BackendServiceException;

    /**
     * Gets the production request for the given production ID.
     *
     * @param productionId The production ID
     *
     * @return The production request, or {@code null} if none with the given ID was found.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoProductionRequest getProductionRequest(String productionId) throws BackendServiceException;

    void cancelProductions(String[] productionIds) throws BackendServiceException;

    void deleteProductions(String[] productionIds) throws BackendServiceException;

    void stageProductions(String[] productionIds) throws BackendServiceException;

    void scpProduction(String productionId, String remotePath) throws BackendServiceException;

    String[] listUserFiles(String dirPath) throws BackendServiceException;

    boolean removeUserFile(String filePath) throws BackendServiceException;

    boolean removeUserDirectory(String filePath) throws BackendServiceException;

    String checkUserRecordSource(String filePath) throws BackendServiceException;

    float[] listUserRecordSource(String filePath) throws BackendServiceException;

    boolean isUserInRole(String role);
}
