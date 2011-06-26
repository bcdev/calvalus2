package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A named geographical region.
 *
 * @author Norman
 */
public class DtoRegion implements IsSerializable {
    private String name;
    private String geometryWkt;

    public DtoRegion() {
    }

    public DtoRegion(String name, String geometryWkt) {
        this.name = name;
        this.geometryWkt = geometryWkt;
    }

    public String getName() {
        return name;
    }

    public String getGeometryWkt() {
        return geometryWkt;
    }
}
