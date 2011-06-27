package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.Dialog;
import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.user.client.ui.Label;

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
            Dialog.showMessage("Delete Regions", "No regions selected.");
        } else if (selectedRegions.length == 1) {
            Region selectedRegion = selectedRegions[0];
            if (!selectedRegion.isUserRegion()) {
                Dialog.showMessage("Delete Regions", "You can only delete your own regions.");
                return;
            }
            Dialog dialog = new Dialog("Delete Regions",
                                       new Label("Really delete region '" + selectedRegion.getName() + "'?"),
                                       Dialog.ButtonType.YES, Dialog.ButtonType.CANCEL);
            Dialog.ButtonType button = dialog.show();
            if (button == Dialog.ButtonType.YES) {
                regionMap.removeRegion(selectedRegion);
            }
        } else {
            Dialog dialog = new Dialog("Delete Regions",
                                       new Label("Really delete " + selectedRegions.length + " regions?"),
                                       Dialog.ButtonType.YES, Dialog.ButtonType.CANCEL);
            Dialog.ButtonType button = dialog.show();
            if (button == Dialog.ButtonType.YES) {
                int n = 0;
                for (Region selectedRegion : selectedRegions) {
                    if (selectedRegion.isUserRegion()) {
                        regionMap.removeRegion(selectedRegion);
                        n++;
                    }
                }
                if (n == 0) {
                    Dialog.showMessage("Delete Regions", "The selected regions could not be deleted.");
                } else if (n < selectedRegions.length) {
                    Dialog.showMessage("Delete Regions", "Some of the selected regions could not be deleted.");
                }
            }
        }
    }
}
