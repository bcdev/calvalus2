package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.maps.client.geom.LatLngBounds;
import com.google.gwt.user.client.Window;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class LocateRegionsAction extends AbstractMapAction {
    public LocateRegionsAction() {
        super("L", "Locate selected region(s)");
    }

    @Override
    public void run(RegionMap regionMap) {
        Region selectedRegion = regionMap.getRegionSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Window.alert("No region selected.");
            return;
        }
        locateSelection(regionMap);
    }

    private void locateSelection(RegionMap regionMap) {
        Region[] regions = regionMap.getRegionSelectionModel().getSelectedRegions();
        LatLngBounds totalBounds = computeBounds(regions);
        int zoomLevel = regionMap.getMapWidget().getBoundsZoomLevel(totalBounds);
        regionMap.getMapWidget().setZoomLevel(zoomLevel);
        regionMap.getMapWidget().panTo(totalBounds.getCenter());
    }

    private LatLngBounds computeBounds(Region[] regions) {
        LatLngBounds totalBounds = LatLngBounds.newInstance();
        for (Region region : regions) {
            LatLngBounds regionBounds = region.getPolygon().getBounds();
            totalBounds.extend(regionBounds.getNorthEast());
            totalBounds.extend(regionBounds.getSouthWest());
        }
        return totalBounds;
    }

}
