package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

public class PortalProductSet implements IsSerializable {
    private String id;
    private String type;
    private String name;

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
