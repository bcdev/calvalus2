package com.bc.calvalus.production;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A production request. Production requests are submitted to the backend service.
 *
 * @author Norman
 */
public class ProductionRequest {
    private final String productionType;
    private final Map<String, String> productionParameters;

    public ProductionRequest(String productionType,
                             String... productionParametersKeyValuePairs) {
        if (productionType == null) {
            throw new NullPointerException("productionType");
        }
        if (productionType.isEmpty()) {
            throw new IllegalArgumentException("productionType.isEmpty()");
        }
        this.productionType = productionType;
        this.productionParameters = new HashMap<String, String>();
        for (int i = 0; i < productionParametersKeyValuePairs.length; i += 2) {
            if (productionParametersKeyValuePairs[i] == null) {
                throw new IllegalArgumentException("productionParametersKeyValuePairs[" + i + "] == null");
            }
            productionParameters.put(productionParametersKeyValuePairs[i], productionParametersKeyValuePairs[i + 1]);
        }
    }

    public ProductionRequest(String productionType, Map<String, String> productionParameters) {
        if (productionType == null) {
            throw new NullPointerException("productionType");
        }
        if (productionParameters == null) {
            throw new NullPointerException("productionParameters");
        }
        this.productionType = productionType;
        this.productionParameters = new HashMap<String, String>(productionParameters);
    }

    public String getProductionType() {
        return productionType;
    }

    public Map<String, String> getProductionParameters() {
        return Collections.unmodifiableMap(productionParameters);
    }
}
