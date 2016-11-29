package com.bc.calvalus.portal.client.map;

import com.google.gwt.ajaxloader.client.ArrayHelper;
import com.google.gwt.maps.client.base.LatLng;
import com.google.gwt.maps.client.base.LatLngBounds;
import com.google.gwt.maps.client.mvc.MVCArray;
import com.google.gwt.maps.client.mvc.MVCArrayCallback;
import com.google.gwt.maps.client.overlays.Marker;
import com.google.gwt.maps.client.overlays.Polygon;
import com.google.gwt.maps.client.overlays.PolygonOptions;
import com.google.gwt.maps.client.overlays.Rectangle;
import com.google.gwt.view.client.ProvidesKey;

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
    private boolean showPolyon;

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
        if (vertices == null && geometryWkt != null) {
            vertices = WKTParser.parse(geometryWkt);
        }
        return vertices;
    }

    public void setVertices(LatLng[] vertices) {
        this.vertices = vertices;
        this.geometryWkt = null;
    }

    public boolean isShowPolyon() {
        return showPolyon;
    }

    public void setShowPolyon(boolean showPolyon) {
        this.showPolyon = showPolyon;
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
        if (!vertices[0].equals(vertices[vertices.length - 1])){
            stringBuilder.append(',');
            stringBuilder.append(vertices[0].getLongitude());
            stringBuilder.append(' ');
            stringBuilder.append(vertices[0].getLatitude());
        }
        stringBuilder.append("))");
        return stringBuilder.toString();
    }

    public Polygon createPolygon() {
        PolygonOptions polygonOptions = PolygonOptions.newInstance();
        polygonOptions.setPaths(ArrayHelper.toJsArray(getVertices()));
        return Polygon.newInstance(polygonOptions);
    }

    public static LatLng[] getVertices(Polygon polygon) {
        MVCArray<LatLng> polygonPath = polygon.getPath();
        LatLng[] points = new LatLng[polygonPath.getLength()];
        for (int i = 0; i < points.length; i++) {
            points[i] = polygonPath.get(i);
        }
        return points;
    }

    public static LatLng[] getVertices(Marker marker) {
        return new LatLng[]{
                marker.getPosition()
        };
    }

    public static LatLng[] getVertices(Rectangle rectangle) {
        LatLngBounds bounds = rectangle.getBounds();
        LatLng[] points = new LatLng[4];
        LatLng northEast = bounds.getNorthEast();
        LatLng southWest = bounds.getSouthWest();
        points[0] = northEast;
        points[1] = LatLng.newInstance(southWest.getLatitude(), northEast.getLongitude());
        points[2] = southWest;
        points[3] = LatLng.newInstance(northEast.getLatitude(), southWest.getLongitude());
        return points;
    }

    public static LatLngBounds getBounds(Polygon polygon) {
        MVCArray<LatLng> polygonPath = polygon.getPath();
        LatLng latLngZero = polygonPath.get(0);
        final LatLngBounds bounds = LatLngBounds.newInstance(latLngZero, latLngZero);
        polygonPath.forEach(new MVCArrayCallback<LatLng>() {
            @Override
            public void forEach(LatLng latLng, int index) {
                if (index > 0) {
                    bounds.extend(latLng);
                }
            }
        });
        return bounds;
    }
}
