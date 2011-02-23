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
    WorkStatus workStatus;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProduction() {
    }

    public PortalProduction(String id, String name) {
        this.id = id;
        this.name = name;
        this.workStatus = new WorkStatus(WorkStatus.State.WAITING, "", 0.0);
    }

    public PortalProduction(String id, String name, WorkStatus workStatus) {
        this.id = id;
        this.name = name;
        this.workStatus = workStatus;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setWorkStatus(WorkStatus workStatus) {
        this.workStatus = workStatus;
    }

    public WorkStatus getWorkStatus() {
        return workStatus;
    }

    @Override
    public String toString() {
        return "PortalProduction{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", workStatus=" + workStatus +
                '}';
    }
}
