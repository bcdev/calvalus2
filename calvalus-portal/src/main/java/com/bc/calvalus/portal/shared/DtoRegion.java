package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A named geographical region.
 *
 * @author Norman
 */
public class DtoRegion implements IsSerializable {
    private String name;
    private String category;
    private String geometryWkt;

    public DtoRegion() {
    }

    public DtoRegion(String name, String category, String geometryWkt) {
        this.name = name;
        this.category = category;
        this.geometryWkt = geometryWkt;
    }

    public String getName() {
        return name;
    }

    public String getCategory() {
        return category;
    }

    public String getGeometryWkt() {
        return geometryWkt;
    }
}
