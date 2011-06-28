package com.bc.calvalus.portal.client.map;

import com.google.gwt.view.client.ListDataProvider;

import java.util.ArrayList;

/**
 * The default impl. of {@link RegionMapModel}.
 *
 * @author Norman Fomferra
 */
public class RegionMapModelImpl implements RegionMapModel {

    private final ListDataProvider<Region> regionProvider;
    private final MapAction[] mapActions;
    private final ArrayList<ChangeListener> changeListeners;

    public RegionMapModelImpl(ListDataProvider<Region> regionProvider, MapAction... mapActions) {
        this.regionProvider = regionProvider;
        this.mapActions = mapActions;
        this.changeListeners = new ArrayList<ChangeListener>();
    }

    @Override
    public MapAction[] getActions() {
        return mapActions;
    }

    @Override
    public ListDataProvider<Region> getRegionProvider() {
        return regionProvider;
    }

    @Override
    public void addChangeListener(ChangeListener changeListener) {
        changeListeners.add(changeListener);
    }

    @Override
    public void fireRegionAdded(RegionMap regionMap, Region region) {
        ChangeEvent changeEvent = new ChangeEvent(regionMap, region);
        for (ChangeListener changeListener : changeListeners) {
            changeListener.onRegionAdded(changeEvent);
        }
    }

    @Override
    public void fireRegionRemoved(RegionMap regionMap, Region region) {
        ChangeEvent changeEvent = new ChangeEvent(regionMap, region);
        for (ChangeListener changeListener : changeListeners) {
            changeListener.onRegionRemoved(changeEvent);
        }
    }

    @Override
    public void fireRegionChanged(RegionMap regionMap, Region region) {
        ChangeEvent changeEvent = new ChangeEvent(regionMap, region);
        for (ChangeListener changeListener : changeListeners) {
            changeListener.onRegionChanged(changeEvent);
        }
    }
}
