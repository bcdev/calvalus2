package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.MapAction;
import com.bc.calvalus.portal.client.map.MapInteraction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.maps.client.event.MapClickHandler;
import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;

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
            List<Region> list = regionMap.getRegionModel().getRegionProvider().getList();
            for (Region region : list) {
                Polygon regionPolygon = regionMap.getPolygon(region);
                if (regionPolygon == polygon) {
                    regionMap.getRegionSelectionModel().clearSelection();
                    regionMap.getRegionSelectionModel().setSelected(region, true);
                    run(regionMap);
                    return;
                }
            }
        }

        regionMap.getRegionSelectionModel().clearSelection();
    }
}
