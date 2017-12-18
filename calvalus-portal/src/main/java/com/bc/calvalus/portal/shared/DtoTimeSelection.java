package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * @author hans
 */
public class DtoTimeSelection implements IsSerializable {
    private String startTime;
    private String endTime;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    DtoTimeSelection() {
    }

    public DtoTimeSelection(String startTime, String endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

}
