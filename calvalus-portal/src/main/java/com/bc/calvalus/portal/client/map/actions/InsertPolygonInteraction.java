package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.MapAction;
import com.bc.calvalus.portal.client.map.MapInteraction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.event.MapClickHandler;
import com.google.gwt.maps.client.event.MapMouseMoveHandler;
import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.maps.client.geom.Point;
import com.google.gwt.maps.client.overlay.Polyline;

/**
 * An interactor that inserts polygons into a map.
 *
 * @author Norman Fomferra
 */
public class InsertPolygonInteraction extends MapInteraction implements MapClickHandler, MapMouseMoveHandler {
    private RegionMap regionMap;
    private Polyline polyline;

    public InsertPolygonInteraction(MapAction insertAction) {
        super(insertAction);
    }

    @Override
    public void attachTo(RegionMap regionMap) {
        this.regionMap = regionMap;
        regionMap.getMapWidget().addMapClickHandler(this);
        regionMap.getMapWidget().addMapMouseMoveHandler(this);
    }

    @Override
    public void detachFrom(RegionMap regionMap) {
        regionMap.getMapWidget().removeMapClickHandler(this);
        regionMap.getMapWidget().removeMapMouseMoveHandler(this);
        this.regionMap = null;
    }

    @Override
    public void onClick(MapClickEvent event) {
        MapWidget mapWidget = event.getSender();
        LatLng latLng = event.getLatLng();
        if (latLng == null) {
            latLng = event.getOverlayLatLng();
            if (latLng == null) {
                return;
            }
        }
        if (polyline == null) {
            polyline = new Polyline(new LatLng[]{latLng, latLng});
            mapWidget.addOverlay(polyline);
        } else {
            Point point1 = mapWidget.convertLatLngToDivPixel(latLng);
            Point point2 = mapWidget.convertLatLngToDivPixel(polyline.getVertex(0));
            int dx = point2.getX() - point1.getX();
            int dy = point2.getY() - point1.getY();
            double pixelDistance = Math.sqrt(dx * dx + dy * dy);
            if (pixelDistance < 8.0) {
                LatLng[] polygonVertices = getPolygonVertices(polyline);
                mapWidget.removeOverlay(polyline);
                polyline = null;
                regionMap.addRegion(Region.createUserRegion(polygonVertices));
                // Interaction complete, invoke the actual action.
                run(regionMap);
            } else {
                polyline.insertVertex(polyline.getVertexCount(), latLng);
            }
        }
    }

    @Override
    public void onMouseMove(MapMouseMoveEvent event) {
        if (polyline != null) {
            polyline.deleteVertex(polyline.getVertexCount() - 1);
            polyline.insertVertex(polyline.getVertexCount(), event.getLatLng());
        }
    }

    static LatLng[] getPolygonVertices(Polyline polyline) {
        int n = polyline.getVertexCount();
        LatLng[] points = new LatLng[n];
        for (int i = 0; i < n - 1; i++) {
            points[i] = polyline.getVertex(i);
        }
        points[n - 1] = polyline.getVertex(0);
        return points;
    }

}
