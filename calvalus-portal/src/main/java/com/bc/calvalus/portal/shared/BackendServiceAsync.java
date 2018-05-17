package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("backend")
public interface BackendServiceAsync {

    void loadRegions(String filter, AsyncCallback<DtoRegion[]> callback);

    void storeRegions(DtoRegion[] regions, AsyncCallback<Void> callback);

    void listUserFiles(String dir, AsyncCallback<String[]> callback);

    void removeUserFile(String path, AsyncCallback<Boolean> callback);

    void removeUserFiles(String[] paths, AsyncCallback<Boolean> callback);

    void removeUserDirectory(String path, AsyncCallback<Boolean> callback);

    void getProductSets(String filter, AsyncCallback<DtoProductSet[]> callback);

    void getColorPaletteSets(String filter, AsyncCallback<DtoColorPaletteSet[]> callback);

    void getProcessors(String filter, AsyncCallback<DtoProcessorDescriptor[]> callback);

    void getAggregators(String filter, AsyncCallback<DtoAggregatorDescriptor[]> callback);

    void getProductionRequest(String productionId, AsyncCallback<DtoProductionRequest> callback);

    void getProductions(String filter, AsyncCallback<DtoProduction[]> callback);

    void orderProduction(DtoProductionRequest request, AsyncCallback<DtoProductionResponse> callback);

    void cancelProductions(String[] productionIds, AsyncCallback<Void> callback);

    void deleteProductions(String[] productionIds, AsyncCallback<Void> callback);

    void stageProductions(String[] productionIds, AsyncCallback<Void> callback);

    void scpProduction(String productionId, String remotePath, AsyncCallback<Void> callback);

    void saveRequest(DtoProductionRequest productionRequest, AsyncCallback<Void> callback);

    void deleteRequest(String requestId, AsyncCallback<Void> callback);

    void listRequests(AsyncCallback<DtoProductionRequest[] > callback);

    void checkUserRecordSource(String s, AsyncCallback<String> callback);

    void listUserRecordSource(String s, AsyncCallback<float[]> callback);

    void listSystemFiles(String baseDir, AsyncCallback<String[]> callback);

    void getCalvalusConfig(AsyncCallback<DtoCalvalusConfig> callback);

    void getMasks(AsyncCallback<DtoMaskDescriptor[]> initMasksCallback);

    void calculateL3Periods(String minDate, String maxDate, String periodLength, String compositingPeriodLength, AsyncCallback<String[][]> callback);

    void loadRegionDataInfo(String filePath, AsyncCallback<DtoRegionDataInfo> callback);
}
