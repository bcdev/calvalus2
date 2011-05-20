package com.bc.calvalus.client.map;

import com.google.gwt.maps.client.overlay.PolyStyleOptions;
import com.google.gwt.view.client.ListDataProvider;

import java.util.*;

/**
 * The default impl. of {@link RegionMapModel}.
 *
 * @author Norman Fomferra
 */
public class RegionMapModelImpl implements RegionMapModel, RegionSelection {

    private final ListDataProvider<Region> regionProvider;
    private final MapAction[] mapActions;
    private final Set<Region> selectedRegions;
    private final PolyStyleOptions selectedPolyStrokeStyle;
    private final PolyStyleOptions normalPolyStrokeStyle;

    public RegionMapModelImpl(List<Region> regionList, MapAction... mapActions) {
        this.mapActions = mapActions;
        this.regionProvider = new ListDataProvider<Region>(regionList);
        this.selectedRegions = new HashSet<Region>();

        selectedPolyStrokeStyle = PolyStyleOptions.newInstance("#FFFF00", 3, 0.7);
        normalPolyStrokeStyle = PolyStyleOptions.newInstance("#0000FF", 3, 0.7);

        markUnselected(regionList);
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
    public final RegionSelection getRegionSelection() {
        return this;
    }

    @Override
    public Region getSelectedRegion() {
        return selectedRegions.isEmpty() ? null : getSelectedRegions()[0];
    }

    @Override
    public Region[] getSelectedRegions() {
        return selectedRegions.toArray(new Region[selectedRegions.size()]);
    }

    @Override
    public void setSelectedRegions(Region... regions) {
        markUnselected(selectedRegions);
        selectedRegions.clear();
        addSelectedRegions(regions);
    }

    @Override
    public void addSelectedRegions(Region... regions) {
        List<Region> regionList = Arrays.asList(regions);
        selectedRegions.addAll(regionList);
        markSelected(regionList);
    }

    @Override
    public void removeSelectedRegions(Region... regions) {
        List<Region> regionList = Arrays.asList(regions);
        markUnselected(regionList);
        selectedRegions.removeAll(regionList);
    }

    private void markSelected(Collection<Region> regions) {
        mark(regions, selectedPolyStrokeStyle, true);
    }

    private void markUnselected(Collection<Region> regions) {
        mark(regions, normalPolyStrokeStyle, false);
    }

    private static void mark(Collection<Region> regions, PolyStyleOptions polyStrokeStyle, boolean editingEnabled) {
        for (Region region : regions) {
            region.getPolygon().setStrokeStyle(polyStrokeStyle);
            if (region.isUserRegion()) {
                region.getPolygon().setEditingEnabled(editingEnabled);
            }
        }
    }

}
