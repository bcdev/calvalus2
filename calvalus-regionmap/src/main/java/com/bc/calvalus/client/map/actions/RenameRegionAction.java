package com.bc.calvalus.client.map.actions;

import com.bc.calvalus.client.map.AbstractMapAction;
import com.bc.calvalus.client.map.Region;
import com.bc.calvalus.client.map.RegionMap;
import com.google.gwt.user.client.Window;

/**
* todo - add api doc
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
