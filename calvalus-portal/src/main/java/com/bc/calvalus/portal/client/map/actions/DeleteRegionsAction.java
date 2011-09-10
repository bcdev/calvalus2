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

    private static final String TITLE = "Delete Regions";

    public DeleteRegionsAction() {
        super("D", "Delete selected region(s)");
    }

    @Override
    public void run(final RegionMap regionMap) {
        final Region selectedRegion = regionMap.getRegionMapSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Dialog.info(TITLE, "No region selected.");
            return;
        }
        if (!selectedRegion.isUserRegion()) {
            Dialog.info(TITLE, "You can only delete your own regions.");
            return;
        }
        Dialog dialog = new Dialog(TITLE,
                                   new Label("Really delete region '" + selectedRegion.getName() + "'?"),
                                   Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
            @Override
            protected void onOk() {
                regionMap.removeRegion(selectedRegion);
                super.onOk();
            }
        };
        dialog.show();
    }
}
