package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A production response. Production responses are the result of a production request sent to the backend service.
 *
 * @author Norman
 */
public class PortalProductionResponse implements IsSerializable {
    PortalProduction production;
    PortalProductionRequest productionRequest;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProductionResponse() {
    }

    public PortalProductionResponse(PortalProduction production, PortalProductionRequest productionRequest) {
        this.production = production;
        this.productionRequest = productionRequest;
    }

    public PortalProduction getProduction() {
        return production;
    }

    public PortalProductionRequest getProductionRequest() {
        return productionRequest;
    }
}
