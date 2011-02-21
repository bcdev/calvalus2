package com.bc.calvalus.portal.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A production request. Production requests are submitted to the backend service.
 *
 * @author Norman
 */
public class PortalProductionRequest implements IsSerializable {
    private String productionType;
    private PortalParameter[] productionParameters;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProductionRequest() {
    }

    public PortalProductionRequest(String productionType,
                                   PortalParameter... productionParameters) {
        this.productionType = productionType;
        this.productionParameters = productionParameters;
    }

    public static boolean isValid(PortalProductionRequest req) {
        if (req.getProductionType() == null || req.getProductionType().isEmpty()) {
            return false;
        }
        return true;
    }

    public String getProductionType() {
        return productionType;
    }

    public PortalParameter[] getProductionParameters() {
        return productionParameters;
    }

}
