package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A named geographical region.
 *
 * @author Norman
 */
public class DtoRegion implements IsSerializable {

    private String name;
    private String[] path;
    private String geometryWkt;

    public DtoRegion() {
    }

    public DtoRegion(String name, String[] path, String geometryWkt) {
        this.name = name;
        this.path = path;
        this.geometryWkt = geometryWkt;
    }

    public String getName() {
        return name;
    }

    public String[] getPath() {
        return path;
    }

    public String getGeometryWkt() {
        return geometryWkt;
    }

    public boolean isUserRegion() {
        return path != null && path.length > 0 && "user".equals(path[0]);
    }

    public String getQualifiedName() {
        StringBuilder sb = new StringBuilder();
        for (String pathElement : path) {
            sb.append(pathElement);
            sb.append(".");
        }
        sb.append(name);
        return sb.toString();
    }
}
