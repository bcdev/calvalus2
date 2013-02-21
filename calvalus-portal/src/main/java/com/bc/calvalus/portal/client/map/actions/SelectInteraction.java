package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.MapAction;
import com.bc.calvalus.portal.client.map.MapInteraction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.ajaxloader.client.Properties;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.maps.client.events.click.ClickMapEvent;
import com.google.gwt.maps.client.events.click.ClickMapHandler;
import com.google.gwt.maps.client.overlays.Polygon;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An interactor that selects polygons on a map.
 *
 * @author Norman Fomferra
 */
public class SelectInteraction extends MapInteraction  {
    private RegionMap regionMap;

    public SelectInteraction(MapAction selectAction) {
        super(selectAction);
    }

    @Override
    public void attachTo(RegionMap regionMap) {
        this.regionMap = regionMap;
    }

    @Override
    public void detachFrom(RegionMap regionMap) {
        this.regionMap = null;
    }
}
