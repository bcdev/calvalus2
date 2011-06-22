package com.bc.calvalus.portal.client.map;

import com.google.gwt.view.client.ListDataProvider;

/**
 * The data and selection model for the {@link RegionMapWidget}.
 *
 * @author Norman Fomferra
 */
public interface RegionMapModel {
    MapAction[] getActions();

    ListDataProvider<Region> getRegionList();
}
