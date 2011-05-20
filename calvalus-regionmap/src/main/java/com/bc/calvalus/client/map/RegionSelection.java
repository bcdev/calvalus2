package com.bc.calvalus.client.map;

/**
 * A region selection model.
 */
public interface RegionSelection {

    Region getSelectedRegion();

    Region[] getSelectedRegions();

    void setSelectedRegions(Region... region);

    void addSelectedRegions(Region... regions);

    void removeSelectedRegions(Region... regions);
}
