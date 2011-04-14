package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.ProductionRequest} class.
 *
 * @author Norman
 */
public class GsProductionRequest implements IsSerializable {
    private String productionType;
    private Map<String, String> productionParameters;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public GsProductionRequest() {
    }

    public GsProductionRequest(String productionType,
                               Map<String, String> productionParameters) {
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
