package com.bc.calvalus.production;

/**
 * A production response. Production responses are the result of a production request.
 *
 * @author Norman
 */
public class ProductionResponse {
    private Production production;

    public ProductionResponse(Production production) {
        if (production == null) {
            throw new NullPointerException("production");
        }
        this.production = production;
    }

    public Production getProduction() {
        return production;
    }
}
