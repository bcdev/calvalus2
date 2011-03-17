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
    private final String userName;
    private final Map<String, String> productionParameters;

    public ProductionRequest(String productionType,
                             String userName,
                             String... productionParametersKeyValuePairs) {
        this(productionType, userName, mapify(productionParametersKeyValuePairs));
    }

    public ProductionRequest(String productionType,
                             String userName,
                             Map<String, String> productionParameters) {
        if (productionType == null) {
            throw new NullPointerException("productionType");
        }
        if (productionType.isEmpty()) {
            throw new IllegalArgumentException("productionType.isEmpty()");
        }
        if (userName == null) {
            throw new NullPointerException("userName");
        }
        if (userName.isEmpty()) {
            throw new IllegalArgumentException("userName.isEmpty()");
        }
        if (productionParameters == null) {
            throw new NullPointerException("productionParameters");
        }
        this.productionType = productionType;
        this.userName = userName;
        this.productionParameters = new HashMap<String, String>(productionParameters);
    }

    public String getProductionType() {
        return productionType;
    }

    public String getUserName() {
        return userName;
    }

    public String getProductionParameter(String name)  {
        return productionParameters.get(name);
    }

    public String getProductionParameterSafe(String name) throws ProductionException {
        String value = getProductionParameter(name);
        if (value == null) {
            throw new ProductionException("Missing production parameter '" + name + "'");
        }
        return value;
    }

    public void ensureProductionParameterSet(String name) throws ProductionException {
        getProductionParameterSafe(name);
    }

    public Map<String, String> getProductionParameters() {
        return Collections.unmodifiableMap(productionParameters);
    }

    private static Map<String, String> mapify(String[] productionParametersKeyValuePairs) {
        Map<String, String> productionParameters = new HashMap<String, String>();
        for (int i = 0; i < productionParametersKeyValuePairs.length; i += 2) {
            String name = productionParametersKeyValuePairs[i];
             if (name == null) {
                throw new NullPointerException("name");
            }
            String value = productionParametersKeyValuePairs[i + 1];
            if (value == null) {
                throw new NullPointerException("value");
            }
            productionParameters.put(name, value);
        }
        return productionParameters;
    }
}
