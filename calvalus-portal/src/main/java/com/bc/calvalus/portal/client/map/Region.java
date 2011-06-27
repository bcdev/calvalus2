package com.bc.calvalus.portal.client.map;

import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.maps.client.overlay.Marker;
import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.maps.client.overlay.Polyline;

/**
 * Basically, a named polygon.
 *
 * @author Norman Fomferra
 */
public class Region {

    private static int counter;

    private String category;
    private String name;
    private String qualifiedName;
    private String geometryWkt;
    private LatLng[] vertices;

    public static Region createUserRegion(LatLng[] polygonVertices) {
        return new Region("region_" + (++counter), "user", polygonVertices);
    }

    public Region(String name, String category, String geometryWkt) {
        this(name, category, geometryWkt, null);
    }

    public Region(String name, String category, LatLng[] vertices) {
        this(name, category, null, vertices);
    }

    private Region(String name, String category, String geometryWkt, LatLng[] vertices) {
        this.name = name;
        this.category = category;
        this.geometryWkt = geometryWkt;
        this.vertices = vertices;
    }

    public String getQualifiedName() {
        if (qualifiedName == null) {
            qualifiedName = category + "." + name;
        }
        return qualifiedName;
    }

    public String getCategory() {
        return category;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        this.qualifiedName = null;
    }

    public String getGeometryWkt() {
        if (this.geometryWkt == null) {
            this.geometryWkt = toWkt(vertices);
        }
        return geometryWkt;
    }

    public boolean isUserRegion() {
        return getCategory().equals("user");
    }

    public LatLng[] getVertices() {
        if (vertices == null) {
            Overlay overlay = WKTParser.parse(geometryWkt);
            vertices = getVertices(overlay);
        }
        return vertices;
    }

    public static String toWkt(LatLng[] polygonVertices) {
        StringBuilder stringBuilder = new StringBuilder("POLYGON((");
        for (int i = 0; i < polygonVertices.length; i++) {
            LatLng point = polygonVertices[i];
            if (i > 0) {
                stringBuilder.append(',');
            }
            stringBuilder.append(point.getLongitude());
            stringBuilder.append(' ');
            stringBuilder.append(point.getLatitude());
        }
        stringBuilder.append("))");
        return stringBuilder.toString();
    }

    public Polygon createPolygon() {
        return new Polygon(getVertices());
    }

    public static LatLng[] getVertices(Overlay overlay) {
        if (overlay instanceof Polygon) {
            Polygon polygon = (Polygon) overlay;
            LatLng[] points = new LatLng[polygon.getVertexCount()];
            for (int i = 0; i < points.length; i++) {
                points[i] = polygon.getVertex(i);
            }
            return points;
        } else if (overlay instanceof Polyline) {
            Polyline polyline = (Polyline) overlay;
            LatLng[] points = new LatLng[polyline.getVertexCount()];
            for (int i = 0; i < points.length; i++) {
                points[i] = polyline.getVertex(i);
            }
            return points;
        } else if (overlay instanceof Marker) {
            Marker marker = (Marker) overlay;
            return new LatLng[]{
                    marker.getLatLng()
            };
        }
        return null;
    }
}
