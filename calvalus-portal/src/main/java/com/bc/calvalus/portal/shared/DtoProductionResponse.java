package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.ProductionResponse} class.
 *
 * @author Norman
 */
public class DtoProductionResponse implements IsSerializable {
    private DtoProduction production;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProductionResponse() {
    }

    public DtoProductionResponse(DtoProduction production) {
        this.production = production;
    }

    public DtoProduction getProduction() {
        return production;
    }
}
