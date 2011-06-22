package com.bc.calvalus.portal.client.map;

import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionModel;

import java.util.HashSet;
import java.util.Set;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class RegionMapSelectionModelImpl
        extends SelectionModel.AbstractSelectionModel<Region>
        implements RegionMapSelectionModel {

    private final Set<Region> selectedRegions;

    private static final ProvidesKey<Region> KEY_PROVIDER = new ProvidesKey<Region>() {
        @Override
        public Object getKey(Region item) {
            return item.getName();
        }
    };

    public RegionMapSelectionModelImpl() {
        super(KEY_PROVIDER);
        this.selectedRegions = new HashSet<Region>();
    }

    @Override
    public boolean isSelected(Region region) {
        return selectedRegions.contains(region);
    }

    @Override
    public void setSelected(Region region, boolean selected) {
        setSelected(region, selected, true);
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
    public void clearSelection() {
        Region[] selectedRegions = getSelectedRegions();
        boolean change = false;
        for (Region selectedRegion : selectedRegions) {
            if (setSelected(selectedRegion, false, false)) {
                change = true;
            }
        }
        if (change) {
            scheduleSelectionChangeEvent();
        }
    }

    private boolean setSelected(Region region, boolean selected, boolean fireEvent) {
        final boolean change;
        if (selected) {
            change = selectedRegions.add(region);
        } else {
            change = selectedRegions.remove(region);
        }
        if (change) {
            if (fireEvent) {
                scheduleSelectionChangeEvent();
            }
        }
        return change;
    }
}
