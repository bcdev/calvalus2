package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.MapAction;
import com.bc.calvalus.portal.client.map.MapInteraction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.maps.client.base.LatLng;
import com.google.gwt.maps.client.drawinglib.DrawingManager;
import com.google.gwt.maps.client.drawinglib.OverlayType;
import com.google.gwt.maps.client.events.overlaycomplete.rectangle.RectangleCompleteMapEvent;
import com.google.gwt.maps.client.events.overlaycomplete.rectangle.RectangleCompleteMapHandler;
import com.google.gwt.maps.client.overlays.Rectangle;

/**
 * An interactor that inserts rectangular polygons into a map.
 *
 * @author Norman Fomferra
 */
public class InsertBoxInteraction extends MapInteraction implements RectangleCompleteMapHandler {
    private final DrawingManager drawingManager;
    private RegionMap regionMap;
    private HandlerRegistration handlerRegistration;

    public InsertBoxInteraction(DrawingManager drawingManager, MapAction insertAction) {
        super(insertAction);
        this.drawingManager = drawingManager;
    }

    @Override
    public void attachTo(RegionMap regionMap) {
        this.regionMap = regionMap;
        drawingManager.setMap(regionMap.getMapWidget());
        drawingManager.setDrawingMode(OverlayType.RECTANGLE);
        handlerRegistration = drawingManager.addRectangleCompleteHandler(this);
    }

    @Override
    public void detachFrom(RegionMap regionMap) {
        drawingManager.setMap(null);
        drawingManager.setDrawingMode(null);
        if (handlerRegistration != null) {
            handlerRegistration.removeHandler();
        }
        this.regionMap = null;
    }

    @Override
    public void onEvent(RectangleCompleteMapEvent event) {
        Rectangle rectangle = event.getRectangle();
        rectangle.setMap(null);
        LatLng[] polygonVertices = Region.getVertices(rectangle);
        regionMap.addRegion(Region.createUserRegion(polygonVertices));
        // Interaction complete, invoke the actual action.
        run(regionMap);
    }
}
