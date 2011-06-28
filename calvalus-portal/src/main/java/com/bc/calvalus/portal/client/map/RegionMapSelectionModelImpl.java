package com.bc.calvalus.portal.client.map;

import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionModel;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class RegionMapSelectionModelImpl
        extends SelectionModel.AbstractSelectionModel<Region>
        implements RegionMapSelectionModel {

    private Region selectedRegion;

    public RegionMapSelectionModelImpl() {
        super(Region.KEY_PROVIDER);
    }

    @Override
    public boolean isSelected(Region region) {
        return selectedRegion == region;
    }

    @Override
    public void setSelected(Region region, boolean selected) {
        if (selected) {
            if (selectedRegion != region) {
                selectedRegion = region;
                scheduleSelectionChangeEvent();
            }
        } else {
            if (selectedRegion == region) {
                selectedRegion = null;
                scheduleSelectionChangeEvent();
            }
        }
    }

    @Override
    public Region getSelectedRegion() {
        return selectedRegion;
    }

    @Override
    public void clearSelection() {
        if (selectedRegion != null) {
            selectedRegion = null;
            scheduleSelectionChangeEvent();
        }
    }
}
