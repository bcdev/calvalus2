package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A production response. Production responses are the result of a production request.
 *
 * @author Norman
 */
public class PortalProductionResponse implements IsSerializable {
    private PortalProduction production;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProductionResponse() {
    }

    public PortalProductionResponse(PortalProduction production) {
        this.production = production;
    }

    public PortalProduction getProduction() {
        return production;
    }
}
