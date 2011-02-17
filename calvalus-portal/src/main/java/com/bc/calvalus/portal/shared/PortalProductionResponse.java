package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

public class PortalProductionResponse implements IsSerializable {
    String message;

    public PortalProductionResponse() {
    }

    public PortalProductionResponse(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
