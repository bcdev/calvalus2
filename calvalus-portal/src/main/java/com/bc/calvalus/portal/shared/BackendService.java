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
     * Gets all known color palettes.
     *
     * @param filter A filter expression (not yet used).
     *
     * @return The array of color palettes.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoColorPalette[] loadColorPalettes(String filter) throws BackendServiceException;

    /**
     * Persists the provided color palettes.
     *
     * @param colorPalettes The color palettes to persist.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    void storeColorPalettes(DtoColorPalette[] colorPalettes) throws BackendServiceException;

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
     * Gets all known aggregators.
     *
     * @param filter A filter expression (not yet used).
     *
     * @return The aggregators.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoAggregatorDescriptor[] getAggregators(String filter) throws BackendServiceException;

    /**
     * Gets the user masks.
     *
     * @return The masks.
     *
     * @throws BackendServiceException If a server error occurred.
     */
    DtoMaskDescriptor[] getMasks() throws BackendServiceException;

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

    void saveRequest(DtoProductionRequest productionRequest) throws BackendServiceException;

    void deleteRequest(String requestId) throws BackendServiceException;

    DtoProductionRequest[] listRequests() throws BackendServiceException;

    /**
     * Lists files within the user's file space in the inventory.
     *
     * @param dirPath     A path that may contain globs ("*" or "?")
     *
     * @return The list of files.
     *
     * @throws BackendServiceException If an error occured.
     */
    String[] listUserFiles(String dirPath) throws BackendServiceException;

    /**
     * Lists files in the inventory.
     *
     * @param dirPath     A path that may contain globs ("*" or "?")
     *
     * @return The list of files.
     *
     * @throws BackendServiceException If an error occured.
     */
    String[] listSystemFiles(String dirPath) throws BackendServiceException;

    /**
     * Deletes a file from the user's file space in the inventory.
     *
     * @param filePath     A relative path into the user's file space.
     *
     * @return true, if the file could be found and removed.
     *
     * @throws BackendServiceException If an error occurred (file exists, but can't be removed).
     */
    boolean removeUserFile(String filePath) throws BackendServiceException;

    /**
     * Deletes multiple files from the user's file space in the inventory.
     *
     * @param filePaths     A list of relative path into the user's file space.
     *
     * @return true, if the files could be found and removed.
     *
     * @throws BackendServiceException If an error occurred (file exists, but can't be removed).
     */
    boolean removeUserFiles(String[] filePaths) throws BackendServiceException;

    /**
     * Deletes a directory from the user's file space in the inventory.
     *
     * @param filePath     A relative path into the user's file space.
     *
     * @return true, if the directory could be found and removed.
     *
     * @throws BackendServiceException If an error occurred (directory exists, but can't be removed).
     */
    boolean removeUserDirectory(String filePath) throws BackendServiceException;

    String checkUserRecordSource(String filePath) throws BackendServiceException;

    float[] listUserRecordSource(String filePath) throws BackendServiceException;

    DtoCalvalusConfig getCalvalusConfig();

    /**
     * Computes the periods as defined by the given parameters.
     * The computation on the frontend is difficult, because "java.util.Calendar" is not supported 
     * in GWT.
     */
    String[][] calculateL3Periods(String minDate, String maxDate, String steppingPeriodLength, String compositingPeriodLength);

    /**
     * Load details about the region data.
     */
    DtoRegionDataInfo loadRegionDataInfo(String filePath) throws BackendServiceException;
}
