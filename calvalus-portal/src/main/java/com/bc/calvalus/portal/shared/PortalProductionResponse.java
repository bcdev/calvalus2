package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

public class PortalProductionResponse implements IsSerializable {
    String productionId;
    String message;

    public PortalProductionResponse() {
    }

    public PortalProductionResponse(String productionId, String message) {
        this.productionId = productionId;
        this.message = message;
    }

    public String getProductionId() {
        return productionId;
    }

    public String getMessage() {
        return message;
    }
}
