package com.bc.calvalus.client.map;

import com.google.gwt.maps.client.MapWidget;

/**
 * A Google map that has regions.
 *
 * @author Norman.
 */
public interface RegionMap {
    /**
     * @return The Google map.
     */
    MapWidget getMapWidget();

    /**
     * @return The region model.
     */
    RegionMapModel getModel();
}
