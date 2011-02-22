package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("backend")
public interface BackendServiceAsync {
    void getProductSets(String type, AsyncCallback<PortalProductSet[]> callback);

    void getProcessors(String type, AsyncCallback<PortalProcessor[]> callback);

    void getProductions(String type, AsyncCallback<PortalProduction[]> callback);

    void orderProduction(PortalProductionRequest request, AsyncCallback<PortalProductionResponse> callback);

    void getProductionStatus(String productionId, AsyncCallback<WorkStatus> callback);

    void cancelProductions(String[] productionIds, AsyncCallback<boolean[]> callback);

    void deleteProductions(String[] productionIds, AsyncCallback<boolean[]> callback);
}
