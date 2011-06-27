package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.user.client.Window;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class DeleteRegionsAction extends AbstractMapAction {
    public DeleteRegionsAction() {
        super("D", "Delete selected region(s)");
    }

    @Override
    public void run(RegionMap regionMap) {
        Region[] selectedRegions = regionMap.getRegionSelectionModel().getSelectedRegions();
        if (selectedRegions.length == 0) {
            Window.alert("No regions selected.");
            return;
        }
        int n = 0;
        for (Region selectedRegion : selectedRegions) {
            if (selectedRegion.isUserRegion()) {
                regionMap.removeRegion(selectedRegion);
                n++;
            }
        }
        if (n == 0) {
            Window.alert("The selected regions could not be deleted.");
        } else if (n < selectedRegions.length) {
            Window.alert("Some of the selected regions could not be deleted.");
        }
    }
}
