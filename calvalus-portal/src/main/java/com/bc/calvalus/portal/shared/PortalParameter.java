package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
* A key-value pair.
*
* @author Norman
*/
public class PortalParameter implements IsSerializable {
    String name;
    String value;

    /**
     * No-arg constructor as required by {@link com.google.gwt.user.client.rpc.IsSerializable}. Don't use directly.
     */
    public PortalParameter() {
    }

    public PortalParameter(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

}
