package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

public class PortalProductionResponse implements IsSerializable {
    String productionId;
    String productionName;
    PortalProductionRequest productionRequest;

    public PortalProductionResponse() {
    }

    public PortalProductionResponse(String productionId, String productionName, PortalProductionRequest productionRequest) {
        this.productionId = productionId;
        this.productionName = productionName;
        this.productionRequest = productionRequest;
    }

    public String getProductionId() {
        return productionId;
    }

    public String getProductionName() {
        return productionName;
    }

    public PortalProductionRequest getProductionRequest() {
        return productionRequest;
    }
}
