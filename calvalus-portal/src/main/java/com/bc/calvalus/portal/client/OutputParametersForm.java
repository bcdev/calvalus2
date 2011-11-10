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
 * This form show setting related to production in general and output.
 *
 * @author Norman
 * @author MarcoZ
 */
public class OutputParametersForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, OutputParametersForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);


    private final boolean showProductRelatedSettings;

    @UiField
    ListBox outputFormat;
    @UiField
    TextBox productionName;
    @UiField
    CheckBox autoStaging;
    @UiField
    CheckBox autoDelete;
    @UiField
    Panel productRelatedPanel;

    public OutputParametersForm(boolean showProductRelatedSettings) {
        this.showProductRelatedSettings = showProductRelatedSettings;
        initWidget(uiBinder.createAndBindUi(this));
        if (showProductRelatedSettings) {
            // todo - get the available output formats from
            setAvailableOutputFormats("BEAM-DIMAP", "NetCDF", "GeoTIFF");
        } else {
            productRelatedPanel.setVisible(false);
        }
    }

    public void validateForm() throws ValidationException {
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        String prodName = getProductionName();
        if (!prodName.isEmpty()) {
            parameters.put("productionName", prodName);
        }
        if (showProductRelatedSettings) {
            parameters.put("outputFormat", getOutputFormat());
            parameters.put("autoStaging", autoStaging.getValue() + "");
            parameters.put("autoDelete", autoDelete.getValue() + "");
        }
        return parameters;
    }

    private void setAvailableOutputFormats(String... formatNames) {
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

    private String getOutputFormat() {
        int index = outputFormat.getSelectedIndex();
        return outputFormat.getValue(index);
    }

    private String getProductionName() {
        return productionName.getValue().trim();
    }
}