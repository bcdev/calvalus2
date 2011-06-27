package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.user.client.Window;

/**
 * An action that lets a user rename a region.
 *
 * @author Norman Fomferra
 */
public class RenameRegionAction extends AbstractMapAction {
    public RenameRegionAction() {
        super("R", "Rename selected region");
    }

    @Override
    public void run(RegionMap regionMap) {
        Region selectedRegion = regionMap.getRegionSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Window.alert("No region selected.");
            return;
        }
        Window.alert("'Rename' not implemented yet.");
    }
}
