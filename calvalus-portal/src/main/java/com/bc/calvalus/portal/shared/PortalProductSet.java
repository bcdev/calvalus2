package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A proxy for a product set.
 *
 * @author Norman
 */
public class PortalProductSet implements IsSerializable {
    private String id;
    private String type;
    private String name;

    /**
     * No-arg constructor as required by {@link IsSerializable}.
     */
    public PortalProductSet() {
    }

    public PortalProductSet(String id, String type, String name) {
        this.id = id;
        this.type = type;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}
