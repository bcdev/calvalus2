package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class OutputParametersForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, OutputParametersForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox outputFormat;
    @UiField
    CheckBox autoStaging;

    public OutputParametersForm() {
        initWidget(uiBinder.createAndBindUi(this));

        setAvailableOutputFormats("BEAM-DIMAP", "NetCDF", "GeoTIFF");
    }

    public void setAvailableOutputFormats(String... formatNames) {
        int selectedIndex = outputFormat.getSelectedIndex();
        outputFormat.clear();
        for (String formatName : formatNames) {
            outputFormat.addItem(formatName);
        }
        if (selectedIndex >= 0 && selectedIndex < formatNames.length) {
            outputFormat.setSelectedIndex(selectedIndex);
        } else {
            outputFormat.setSelectedIndex(0);
        }
    }

    public String getOutputFormat() {
        int index = outputFormat.getSelectedIndex();
        return outputFormat.getValue(index);
    }

    public boolean isAutoStaging() {
        return autoStaging.getValue();
    }

    public void validateForm() throws ValidationException {
    }
}