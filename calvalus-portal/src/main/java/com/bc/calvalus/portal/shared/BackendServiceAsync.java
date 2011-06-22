package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("backend")
public interface BackendServiceAsync {
    void getRegions(String filter, AsyncCallback<GsRegion[]> callback);

    void getProductSets(String filter, AsyncCallback<GsProductSet[]> callback);

    void getProcessors(String filter, AsyncCallback<GsProcessorDescriptor[]> callback);

    void getProductionRequest(String productionId, AsyncCallback<GsProductionRequest> callback);

    void getProductions(String filter, AsyncCallback<GsProduction[]> callback);

    void orderProduction(GsProductionRequest request, AsyncCallback<GsProductionResponse> callback);

    void cancelProductions(String[] productionIds, AsyncCallback<Void> callback);

    void deleteProductions(String[] productionIds, AsyncCallback<Void> callback);

    void stageProductions(String[] productionIds, AsyncCallback<Void> callback);

}
