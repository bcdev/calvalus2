package com.bc.calvalus.portal.client.map;

import com.google.gwt.view.client.ListDataProvider;

import java.util.List;

/**
 * The default impl. of {@link RegionMapModel}.
 *
 * @author Norman Fomferra
 */
public class RegionMapModelImpl implements RegionMapModel {

    private final ListDataProvider<Region> regionList;
    private final MapAction[] mapActions;

    public RegionMapModelImpl(List<Region> regionList, MapAction... mapActions) {
        this.mapActions = mapActions;
        this.regionList = new ListDataProvider<Region>(regionList);
    }

    public RegionMapModelImpl(ListDataProvider<Region> regionList, MapAction... mapActions) {
        this.mapActions = mapActions;
        this.regionList = regionList;
    }

    @Override
    public MapAction[] getActions() {
        return mapActions;
    }

    @Override
    public ListDataProvider<Region> getRegionList() {
        return regionList;
    }
}
