package com.bc.calvalus.portal.client.map;

import com.google.gwt.view.client.SelectionModel;

/**
 * A region selection model.
 */
public interface RegionMapSelectionModel extends SelectionModel<Region> {

    Region getSelectedRegion();

    Region[] getSelectedRegions();

    void clearSelection();
}
