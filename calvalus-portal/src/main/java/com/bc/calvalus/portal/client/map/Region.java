package com.bc.calvalus.portal.client.map;

import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.maps.client.overlay.Marker;
import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.maps.client.overlay.Polyline;
import com.google.gwt.view.client.ProvidesKey;

import java.util.Arrays;

/**
 * Basically, a named polygon.
 *
 * @author Norman Fomferra
 */
public class Region {

    public static final ProvidesKey<Region> KEY_PROVIDER = new ProvidesKey<Region>() {
        @Override
        public Object getKey(Region region) {
            return region.getQualifiedName();
        }
    };


    private static int counter;

    private String[] path;
    private String name;
    private String qualifiedName;
    private String geometryWkt;
    private LatLng[] vertices;

    public static Region createUserRegion(LatLng[] polygonVertices) {
        return new Region("region_" + (++counter), new String[]{"user"}, polygonVertices);
    }

    public Region(String name, String[] path, String geometryWkt) {
        this(name, path, geometryWkt, null);
    }

    public Region(String name, String[] path, LatLng[] vertices) {
        this(name, path, null, vertices);
    }

    private Region(String name, String[] path, String geometryWkt, LatLng[] vertices) {
        this.name = name;
        this.path = path;
        this.geometryWkt = geometryWkt;
        this.vertices = vertices;
    }

    public String getQualifiedName() {
        if (qualifiedName == null) {
            StringBuilder sb = new StringBuilder();
            for (String pathElement : path) {
                sb.append(pathElement);
                sb.append(".");
            }
            sb.append(name);
            qualifiedName = sb.toString();
        }
        return qualifiedName;
    }

    public void setQualifiedName(String qualifiedName) {
        int lastDot = qualifiedName.lastIndexOf(".");
        String name = qualifiedName.substring(lastDot + 1, qualifiedName.length());
        String[] path = qualifiedName.substring(0, lastDot).split("\\.");
        setName(name);
        setPath(path);
    }

    public String[] getPath() {
        return path;
    }

    public void setPath(String[] path) {
        this.path = path;
        qualifiedName = null;
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
        return path != null && path.length > 0 && "user".equals(path[0]);
    }

    public LatLng[] getVertices() {
        if (vertices == null) {
            Overlay overlay = WKTParser.parse(geometryWkt);
            vertices = getVertices(overlay);
        }
        return vertices;
    }

    public void setVertices(LatLng[] vertices) {
        this.vertices = vertices;
        this.geometryWkt = null;
    }

    public static String toWkt(LatLng[] vertices) {
        StringBuilder stringBuilder = new StringBuilder("POLYGON((");
        for (int i = 0; i < vertices.length; i++) {
            LatLng point = vertices[i];
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
