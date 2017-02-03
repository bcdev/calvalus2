package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.Dialog;
import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.core.client.GWT;
import com.google.gwt.maps.client.base.LatLngBounds;
import com.google.gwt.maps.client.overlays.Polygon;
import com.google.gwt.resources.client.ClientBundle;
import com.google.gwt.resources.client.ImageResource;
import com.google.gwt.user.client.ui.Image;

/**
 * An action that locates the selected regions in the map by zooming to them.
 *
 * @author Norman Fomferra
 */
public class LocateRegionsAction extends AbstractMapAction {
    public LocateRegionsAction() {
        super("L", new Image(((Icons) GWT.create(Icons.class)).getIcon()), "Locate selected region(s)");
    }

    interface Icons extends ClientBundle {
        @Source("RegionLocate24.gif")
        ImageResource getIcon();
    }

    @Override
    public void run(RegionMap regionMap) {
        Region selectedRegion = regionMap.getRegionMapSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Dialog.error("Warning", "No region selected.<br/>Please select a region first.");
            return;
        }
        locateSelectedRegion(regionMap);
    }

    public static void locateSelectedRegion(RegionMap regionMap) {
        Region selectedRegion = regionMap.getRegionMapSelectionModel().getSelectedRegion();
        if (selectedRegion != null) {
            locateRegion(regionMap, selectedRegion);
        }
    }

    public static void locateRegion(RegionMap regionMap, Region region) {
        Polygon regionPolygon = regionMap.getPolygon(region);
        if (regionPolygon == null) {
            regionPolygon = region.createPolygon();
        }
        LatLngBounds bounds = Region.getBounds(regionPolygon);
        regionMap.getMapWidget().fitBounds(bounds);
        regionMap.getMapWidget().panTo(bounds.getCenter());
    }
}
