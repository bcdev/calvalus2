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
    private String userName;
    private Map<String, String> productionParameters;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public GsProductionRequest() {
    }

    public GsProductionRequest(String productionType,
                               String userName,
                               String... productionParametersKeyValuePairs) {
        this(productionType, userName, mapify(productionParametersKeyValuePairs));
    }

    public GsProductionRequest(String productionType,
                               String userName,
                               Map<String, String> productionParameters) {
        if (productionType == null) {
            throw new NullPointerException("productionType");
        }
        if (userName == null) {
            throw new NullPointerException("userName");
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
