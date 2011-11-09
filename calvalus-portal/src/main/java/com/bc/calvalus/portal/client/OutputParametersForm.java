package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Panel;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

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
    TextBox outputDir;
    @UiField
    CheckBox autoStaging;
    @UiField
    CheckBox autoDelete;
    @UiField
    Panel productPanel;

    public OutputParametersForm(boolean showProductSettings) {
        initWidget(uiBinder.createAndBindUi(this));
        if (showProductSettings) {
            // todo - get the available output formats from
            setAvailableOutputFormats("BEAM-DIMAP", "NetCDF", "GeoTIFF");
        } else {
            productPanel.setVisible(false);
        }
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

    public void validateForm() throws ValidationException {
        String value = outputDir.getText().trim();
        boolean outputDirValid = !value.startsWith("/");
        if (!outputDirValid) {
             throw new ValidationException(outputDir, "The output directory must not be an absolute path.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        if (!getOutputDir().isEmpty()) {
            parameters.put("outputDir", getOutputDir());
        }
        parameters.put("outputFormat", getOutputFormat());
        parameters.put("autoStaging", autoStaging.getValue() + "");
        parameters.put("autoDelete", autoDelete.getValue() + "");
        return parameters;
    }

    private String getOutputDir() {
        return outputDir.getValue().trim();
    }

}