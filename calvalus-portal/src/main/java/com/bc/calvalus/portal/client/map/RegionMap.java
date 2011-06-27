package com.bc.calvalus.portal.client.map;

import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.overlay.Polygon;

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
    RegionMapModel getRegionModel();


    /**
     * @return The region selection model.
     */
    RegionMapSelectionModel getRegionSelectionModel();

    /**
     * Gets the region with the given qualified name.
     *
     * @param qualifiedName The qualified region name.
     * @return The region, or {@code null} if no such region exists.
     */
    Region getRegion(String qualifiedName);

    /**
     * Gets the region that is graphically represented in the map by the given polygon.
     *
     * @param polygon The polygon.
     * @return The region, or {@code null} if no such region exists.
     */
    Region getRegion(Polygon polygon);

    /**
     * Gets the polygon used to graphically represent the given region in the map.
     *
     * @param region The region.
     * @return The polygon, or {@code null} if no such polygon exists.
     */
    Polygon getPolygon(Region region);


    /**
     * Adds a new region.
     *
     * @param region The region.
     */
    void addRegion(Region region);

    /**
     * Removes the given region.
     *
     * @param region The region.
     */
    void removeRegion(Region region);

    MapInteraction getCurrentInteraction();

    void setCurrentInteraction(MapInteraction mapInteraction);
}
