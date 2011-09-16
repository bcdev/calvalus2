package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.Dialog;
import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.maps.client.geom.LatLngBounds;
import com.google.gwt.maps.client.overlay.Polygon;

/**
 * An action that locates the selected regions in the map by zooming to them.
 *
 * @author Norman Fomferra
 */
public class LocateRegionsAction extends AbstractMapAction {
    public LocateRegionsAction() {
        super("L", "Locate selected region(s)");
    }

    @Override
    public void run(RegionMap regionMap) {
        Region selectedRegion = regionMap.getRegionMapSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Dialog.error("Warning", "No region selected.<br/>Please select a region first.");
            return;
        }
        locateSelection(regionMap);
    }

    private void locateSelection(RegionMap regionMap) {
        Region selectedRegion = regionMap.getRegionMapSelectionModel().getSelectedRegion();
        if (selectedRegion != null) {
            Polygon regionPolygon = regionMap.getPolygon(selectedRegion);
            if (regionPolygon != null) {
                LatLngBounds bounds = regionPolygon.getBounds();
                int zoomLevel = regionMap.getMapWidget().getBoundsZoomLevel(bounds);
                regionMap.getMapWidget().setZoomLevel(zoomLevel);
                regionMap.getMapWidget().panTo(bounds.getCenter());
            }
        }
    }

}
