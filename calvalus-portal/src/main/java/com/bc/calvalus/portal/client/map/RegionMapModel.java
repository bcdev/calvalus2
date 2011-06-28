package com.bc.calvalus.portal.client.map;

import com.google.gwt.view.client.ListDataProvider;

/**
 * The data and selection model for the {@link RegionMapWidget}.
 *
 * @author Norman Fomferra
 */
public interface RegionMapModel {
    MapAction[] getActions();

    ListDataProvider<Region> getRegionProvider();

    void addChangeListener(ChangeListener changeListener);

    void fireRegionAdded(RegionMap regionMap, Region region);

    void fireRegionRemoved(RegionMap regionMap, Region region);

    void fireRegionChanged(RegionMap regionMap, Region region);

    public static class ChangeEvent {
        private final RegionMap regionMap;
        private final Region region;

        public ChangeEvent(RegionMap regionMap, Region region) {
            this.regionMap = regionMap;
            this.region = region;
        }

        public RegionMap getRegionMap() {
            return regionMap;
        }

        public Region getRegion() {
            return region;
        }
    }

    public interface ChangeListener {
        void onRegionAdded(ChangeEvent event);

        void onRegionRemoved(ChangeEvent event);

        void onRegionChanged(ChangeEvent event);
    }
}
