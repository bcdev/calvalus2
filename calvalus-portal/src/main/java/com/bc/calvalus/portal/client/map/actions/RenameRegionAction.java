package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.Dialog;
import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.core.client.GWT;
import com.google.gwt.resources.client.ClientBundle;
import com.google.gwt.resources.client.ImageResource;
import com.google.gwt.user.client.ui.Image;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;

/**
 * An action that lets a user rename a region.
 *
 * @author Norman Fomferra
 */
public class RenameRegionAction extends AbstractMapAction {

    private static final String TITLE = "Rename Region";

    public RenameRegionAction() {
        super("R", new Image(((Icons) GWT.create(Icons.class)).getIcon()), "Rename selected region");
    }

    interface Icons extends ClientBundle {
        @Source("RegionRename24.gif")
        ImageResource getIcon();
    }

    @Override
    public void run(final RegionMap regionMap) {
        final Region selectedRegion = regionMap.getRegionMapSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Dialog.info(TITLE, "No user region selected.");
            return;
        }
        if (!selectedRegion.isUserRegion()) {
            Dialog.info(TITLE, "You can only rename your own regions.");
            return;
        }

        final TextBox nameBox = new TextBox();
        nameBox.setVisibleLength(32);
        nameBox.setValue(selectedRegion.getQualifiedName().substring("user.".length()));

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.setSpacing(2);
        verticalPanel.add(new Label("New region name:"));
        verticalPanel.add(nameBox);

        Dialog dialog = new Dialog(TITLE, verticalPanel, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
            @Override
            protected void onOk() {
                String value = nameBox.getValue();
                if (value.isEmpty()) {
                    Dialog.info(TITLE, "Please provide a name.");
                    return;
                }
                String qualifiedName = "user." + value;
                if (regionMap.getRegion(qualifiedName) != null) {
                    Dialog.info(TITLE, "A user region with this name already exists.");
                    return;
                }
                selectedRegion.setQualifiedName(qualifiedName);
                regionMap.getRegionModel().fireRegionChanged(regionMap, selectedRegion);
                super.onOk();
            }
        };
        dialog.show();
    }
}
