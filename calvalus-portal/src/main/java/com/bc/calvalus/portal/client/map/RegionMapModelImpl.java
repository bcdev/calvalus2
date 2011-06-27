package com.bc.calvalus.portal.client.map;

import com.google.gwt.view.client.ListDataProvider;

import java.util.List;

/**
 * The default impl. of {@link RegionMapModel}.
 *
 * @author Norman Fomferra
 */
public class RegionMapModelImpl implements RegionMapModel {

    private final ListDataProvider<Region> regionProvider;
    private final MapAction[] mapActions;

    public RegionMapModelImpl(List<Region> regionProvider, MapAction... mapActions) {
        this.regionProvider = new ListDataProvider<Region>(regionProvider);
        this.mapActions = mapActions;
    }

    public RegionMapModelImpl(ListDataProvider<Region> regionProvider, MapAction... mapActions) {
        this.regionProvider = regionProvider;
        this.mapActions = mapActions;
    }

    @Override
    public MapAction[] getActions() {
        return mapActions;
    }

    @Override
    public ListDataProvider<Region> getRegionProvider() {
        return regionProvider;
    }
}
