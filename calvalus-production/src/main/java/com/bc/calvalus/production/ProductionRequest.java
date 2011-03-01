package com.bc.calvalus.production;


/**
 * A production request. Production requests are submitted to the backend service.
 *
 * @author Norman
 */
public class ProductionRequest {
    private String productionType;
    private ProductionParameter[] productionParameters;

    public ProductionRequest(String productionType,
                             ProductionParameter... productionParameters) {
        if (productionType == null) {
            throw new NullPointerException("productionType");
        }
        if (productionType.isEmpty()) {
            throw new IllegalArgumentException("productionType.isEmpty()");
        }
        for (int i = 0; i < productionParameters.length; i++) {
            if (productionParameters[i] == null) {
                throw new IllegalArgumentException("productionParameters[" + i + "] == null");
            }
        }
        this.productionType = productionType;
        this.productionParameters = productionParameters;
    }

    public String getProductionType() {
        return productionType;
    }

    public ProductionParameter[] getProductionParameters() {
        return productionParameters;
    }
}
