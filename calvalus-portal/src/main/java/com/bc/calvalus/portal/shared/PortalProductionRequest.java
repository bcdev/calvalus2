package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A production request. Production requests are submitted to the backend service.
 *
 * @author Norman
 */
public class PortalProductionRequest implements IsSerializable {
    private String productionType;
    private Map<String, String> productionParameters;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProductionRequest() {
    }

    public PortalProductionRequest(String productionType,
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

    public PortalProductionRequest(String productionType, Map<String, String> productionParameters) {
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


    public static boolean isValid(PortalProductionRequest req) {
        if (req.getProductionType() == null || req.getProductionType().isEmpty()) {
            return false;
        }
        return true;
    }



}
