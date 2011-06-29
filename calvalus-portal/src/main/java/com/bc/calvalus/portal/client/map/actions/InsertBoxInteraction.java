package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.MapAction;
import com.bc.calvalus.portal.client.map.MapInteraction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.event.MapClickHandler;
import com.google.gwt.maps.client.event.MapMouseMoveHandler;
import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.maps.client.overlay.Polyline;

/**
 * An interactor that inserts rectangular polygons into a map.
 *
 * @author Norman Fomferra
 */
public class InsertBoxInteraction extends MapInteraction implements MapClickHandler, MapMouseMoveHandler {
    private RegionMap regionMap;
    private Polyline polyline;

    public InsertBoxInteraction(MapAction insertAction) {
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
        LatLng latLng = getLatLng(event);
        if (latLng == null) {
            return;
        }
        if (polyline == null) {
            polyline = new Polyline(new LatLng[]{
                    latLng,
                    latLng,
                    latLng,
                    latLng,
                    latLng,
            });
            mapWidget.addOverlay(polyline);
        } else {
            LatLng[] polygonVertices = {
                    polyline.getVertex(0),
                    polyline.getVertex(1),
                    polyline.getVertex(2),
                    polyline.getVertex(3),
                    polyline.getVertex(4),
            };
            mapWidget.removeOverlay(polyline);
            polyline = null;
            regionMap.addRegion(Region.createUserRegion(polygonVertices));
            // Interaction complete, invoke the actual action.
            run(regionMap);
        }
    }

    @Override
    public void onMouseMove(MapMouseMoveEvent event) {
        if (polyline != null) {
            LatLng latLng1 = polyline.getVertex(0);
            LatLng latLng2 = event.getLatLng();
            polyline.deleteVertex(1);
            polyline.insertVertex(1, LatLng.newInstance(latLng1.getLatitude(), latLng2.getLongitude()));
            polyline.deleteVertex(2);
            polyline.insertVertex(2, LatLng.newInstance(latLng2.getLatitude(), latLng2.getLongitude()));
            polyline.deleteVertex(3);
            polyline.insertVertex(3, LatLng.newInstance(latLng2.getLatitude(), latLng1.getLongitude()));
            polyline.deleteVertex(4);
            polyline.insertVertex(4, LatLng.newInstance(latLng1.getLatitude(), latLng1.getLongitude()));
        }
    }

    private static LatLng getLatLng(MapClickEvent event) {
        LatLng latLng = event.getLatLng();
        if (latLng == null) {
            latLng = event.getOverlayLatLng();
            if (latLng == null) {
                return null;
            }
        }
        return latLng;
    }
}
