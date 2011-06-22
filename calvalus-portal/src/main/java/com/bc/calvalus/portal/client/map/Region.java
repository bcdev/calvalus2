package com.bc.calvalus.portal.client.map;

import com.google.gwt.maps.client.geom.LatLng;
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
    private String wkt;
    private Polygon polygon;

    public static Region createUserRegion(Polygon polygon) {
        return new Region("user.polygon_" + (++counter), polygon);
    }

    public Region(String name, Polygon polygon) {
        this.name = name;
        this.polygon = polygon;
    }

    public Region(String name, String wkt, Polygon polygon) {
        this.name = name;
        this.wkt = wkt;
        this.polygon = polygon;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getWkt() {
        if (this.wkt == null) {
           this.wkt = toWkt(polygon);
        }
        return wkt;
    }

    public void setWkt(String wkt) {
        this.wkt = wkt;
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
            return new Region(regionName, regionWkt, (Polygon) overlay);
        }
        return null;
    }

    public static String toWkt(Polygon polygon) {
        int vertexCount = polygon.getVertexCount();
        for (int i = 0; i < vertexCount; i++) {
            LatLng vertex = polygon.getVertex(i);

        }
        // todo - test and impl. (nf)
        return "";
    }

}
