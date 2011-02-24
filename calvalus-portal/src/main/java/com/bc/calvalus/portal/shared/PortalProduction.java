package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
* Information about a production.
*
* @author Norman
*/
public class PortalProduction implements IsSerializable {
    String id;
    String name;
    PortalProductionStatus status;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProduction() {
    }

    public PortalProduction(String id, String name, PortalProductionStatus status) {
        if (id == null) {
            throw new NullPointerException("id");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (status == null) {
            throw new NullPointerException("status");
        }
        this.id = id;
        this.name = name;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setStatus(PortalProductionStatus status) {
        this.status = status;
    }

    public PortalProductionStatus getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "PortalProduction{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", status=" + status +
                '}';
    }
}
