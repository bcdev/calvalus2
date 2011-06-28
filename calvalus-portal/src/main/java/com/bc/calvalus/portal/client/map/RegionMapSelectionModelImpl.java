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

    private static final ProvidesKey<Region> KEY_PROVIDER = new ProvidesKey<Region>() {
        @Override
        public Object getKey(Region item) {
            return item.getQualifiedName();
        }
    };

    public RegionMapSelectionModelImpl() {
        super(KEY_PROVIDER);
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
                fireSelectionChangeEvent();
            }
        } else {
            if (selectedRegion == region) {
                selectedRegion = null;
                fireSelectionChangeEvent();
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
