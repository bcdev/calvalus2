package com.bc.calvalus.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

public class EncodedRegion implements IsSerializable {
    private String name;
    private String wkt;

    @SuppressWarnings({"UnusedDeclaration"})
    public EncodedRegion() {
    }

    public EncodedRegion(String name, String wkt) {
        this.name = name;
        this.wkt = wkt;
    }

    public String getName() {
        return name;
    }

    public String getWkt() {
        return wkt;
    }
}
