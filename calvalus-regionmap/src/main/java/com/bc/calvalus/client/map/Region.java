package com.bc.calvalus.client.map;

import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;

/**
 * Basically, a named polygon.
 *
 * @author Norman Fomferra
 */
public class Region {
    private static int counter;

    private String name;
    private Polygon polygon;

    public static Region createUserRegion(Polygon polygon) {
        return new Region("user.polygon_" + (++counter), polygon);
    }

    public Region(String name, Polygon polygon) {
        this.name = name;
        this.polygon = polygon;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isUserRegion() {
        return getName().startsWith("user.");
    }

    public Polygon getPolygon() {
        return polygon;
    }

    public void setPolygon(Polygon polygon) {
        this.polygon = polygon;
    }

    public static Region fromWKT(String regionName, String regionWkt) {
        Overlay overlay = WKTParser.parse(regionWkt);
        if (overlay instanceof Polygon) {
            return new Region(regionName, (Polygon) overlay);
        }
        return null;
    }

}
