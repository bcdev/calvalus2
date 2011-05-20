package com.bc.calvalus.client.map.interactions;

import com.bc.calvalus.client.map.MapAction;
import com.bc.calvalus.client.map.MapInteraction;
import com.bc.calvalus.client.map.Region;
import com.bc.calvalus.client.map.RegionMap;
import com.google.gwt.maps.client.event.MapClickHandler;
import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.maps.client.overlay.Polyline;

import java.util.List;

/**
 * An interactor that inserts rectangular polygons into a map.
 *
 * @author Norman Fomferra
 */
public class SelectInteraction extends MapInteraction implements MapClickHandler {
    private RegionMap regionMap;

    public SelectInteraction(MapAction selectAction) {
        super(selectAction);
    }

    @Override
    public void attachTo(RegionMap regionMap) {
        this.regionMap = regionMap;
        regionMap.getMapWidget().addMapClickHandler(this);
    }

    @Override
    public void detachFrom(RegionMap regionMap) {
        regionMap.getMapWidget().removeMapClickHandler(this);
        this.regionMap = null;
    }

    @Override
    public void onClick(MapClickEvent event) {
        Overlay overlay = event.getOverlay();
        if (overlay instanceof Polygon) {
            Polygon polygon = (Polygon) overlay;
            List<Region> list = regionMap.getModel().getRegionProvider().getList();
            for (Region region : list) {
                if (region.getPolygon() == polygon) {
                    regionMap.getModel().getRegionSelection().setSelectedRegions(region);
                    run(regionMap);
                    return;
                }
            }
        }
        regionMap.getModel().getRegionSelection().setSelectedRegions();
    }
}
