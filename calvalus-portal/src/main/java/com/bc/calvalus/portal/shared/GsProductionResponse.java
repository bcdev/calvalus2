package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.ProductionResponse} class.
 *
 * @author Norman
 */
public class GsProductionResponse implements IsSerializable {
    private GsProduction production;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public GsProductionResponse() {
    }

    public GsProductionResponse(GsProduction production) {
        this.production = production;
    }

    public GsProduction getProduction() {
        return production;
    }
}
