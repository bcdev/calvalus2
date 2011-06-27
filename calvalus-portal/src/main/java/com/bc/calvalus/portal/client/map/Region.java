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

    public static final String USER_PREFIX = "user.";
    private static int counter;

    private String name;
    private String polygonWkt;
    private LatLng[] polygonVertices;

    public static Region createUserRegion(LatLng[] polygonVertices) {
        return new Region(getAbsUserRegionName("region_" + (++counter)), polygonVertices);
    }

    public static String getAbsUserRegionName(String name) {
        return USER_PREFIX + name;
    }

    public static String getRelUserRegionName(String fullName) {
        if (fullName.startsWith(USER_PREFIX)) {
            return fullName.substring(USER_PREFIX.length());
        }
        return fullName;
    }

    public Region(String name, LatLng[] polygonVertices) {
        this.name = name;
        this.polygonVertices = polygonVertices;
    }

    public Region(String name, String polygonWkt, LatLng[] polygonVertices) {
        this.name = name;
        this.polygonWkt = polygonWkt;
        this.polygonVertices = polygonVertices;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPolygonWkt() {
        if (this.polygonWkt == null) {
           this.polygonWkt = toWkt(polygonVertices);
        }
        return polygonWkt;
    }

    public void setPolygonWkt(String polygonWkt) {
        this.polygonWkt = polygonWkt;
    }

    public boolean isUserRegion() {
        return getName().startsWith(USER_PREFIX);
    }

    public LatLng[] getPolygonVertices() {
        return polygonVertices;
    }

    public void setPolygonVertices(LatLng[] polygonVertices) {
        this.polygonVertices = polygonVertices;
    }

    public static Region fromWKT(String regionName, String regionWkt) {
        Overlay overlay = WKTParser.parse(regionWkt);
        if (overlay instanceof Polygon) {
            return new Region(regionName, regionWkt, getPolygonVertices((Polygon) overlay));
        }
        return null;
    }

    public static String toWkt(LatLng[] polygonVertices) {
        // todo - test and impl. (nf)
        return "";
    }

    public Polygon createPolygon() {
        return new Polygon(polygonVertices);
    }

    public static LatLng[] getPolygonVertices(Polygon polygon) {
        LatLng[] points = new LatLng[polygon.getVertexCount()];
        for (int i = 0; i < points.length; i++) {
            points[i] = polygon.getVertex(i);
        }
        return points;
    }


}
