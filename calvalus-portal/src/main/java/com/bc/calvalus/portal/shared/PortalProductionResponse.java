package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A production response. Production responses are the result of a production request sent to the backend service.
 *
 * @author Norman
 */
public class PortalProductionResponse implements IsSerializable {
    private PortalProduction production;
    private int statusCode;
    private String statusMessage;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProductionResponse() {
    }

    public PortalProductionResponse(PortalProduction production) {
        this.production = production;
        this.statusCode = 0;
        this.statusMessage = "";
    }

    public PortalProductionResponse(int statusCode, String statusMessage) {
        this.production = null;
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public PortalProduction getProduction() {
        return production;
    }
}
