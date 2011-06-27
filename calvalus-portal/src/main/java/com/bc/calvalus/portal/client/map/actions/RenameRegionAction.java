package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.Dialog;
import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;

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
    public void run(final RegionMap regionMap) {
        final Region selectedRegion = regionMap.getRegionSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Window.alert("No user region selected.");
            return;
        }
        if (!selectedRegion.isUserRegion()) {
            Window.alert("You can only rename your own regions.");
            return;
        }

        final TextBox nameBox = new TextBox();
        nameBox.setVisibleLength(32);
        nameBox.setValue(selectedRegion.getName());

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.setSpacing(2);
        verticalPanel.add(new Label("New region name:"));
        verticalPanel.add(nameBox);

        Dialog dialog = new Dialog("Rename Region", verticalPanel, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
            @Override
            protected void onOk() {
                String value = nameBox.getValue();
                if (value.isEmpty()) {
                    Dialog.showMessage("Rename Region", "Please provide a name.");
                    return;
                }
                if (regionMap.getRegion(value) != null) {
                    Dialog.showMessage("Rename Region", "A region with this name already exists.");
                    return;
                }
                selectedRegion.setName(value);
                regionMap.getRegionModel().getRegionProvider().refresh();
                super.onOk();
            }
        };
        dialog.show();
    }
}
