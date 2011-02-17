package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("backend")
public interface BackendServiceAsync {
    void getProductSets(String type, AsyncCallback<PortalProductSet[]> async);

    void getProcessors(String type, AsyncCallback<PortalProcessor[]> async);

    void orderProduction(PortalProductionRequest request, AsyncCallback<PortalProductionResponse> callback);
}
