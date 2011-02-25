package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("backend")
public interface BackendServiceAsync {
    void getProductSets(String filter, AsyncCallback<PortalProductSet[]> callback);

    void getProcessors(String filter, AsyncCallback<PortalProcessor[]> callback);

    void getProductions(String filter, AsyncCallback<PortalProduction[]> callback);

    void orderProduction(PortalProductionRequest request, AsyncCallback<PortalProductionResponse> callback);

    void cancelProductions(String[] productionIds, AsyncCallback<Void> callback);

    void deleteProductions(String[] productionIds, AsyncCallback<Void> callback);
}
