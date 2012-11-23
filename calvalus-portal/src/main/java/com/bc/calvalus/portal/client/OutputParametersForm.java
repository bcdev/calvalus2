package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Panel;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextArea;
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


    private boolean showProductRelatedSettings;
    private boolean showTailoringRelatedSettings;
    private boolean showProcessingTypeSettings;

    @UiField
    TextBox productionName;

    @UiField
    Panel processingTypePanel;
    @UiField
    RadioButton processingTypeCluster;
    @UiField
    RadioButton processingTypeUser;

    @UiField
    Panel tailoringPanel;
    @UiField
    TextArea crsText;
    @UiField
    ListBox bandList;
    @UiField
    CheckBox quicklooks;

    @UiField
    Panel productRelatedPanel;
    @UiField
    ListBox outputFormat;
    @UiField
    CheckBox autoStaging;
    @UiField
    CheckBox autoDelete; // currently unused

    static int radioGroupId;

    public OutputParametersForm() {
        initWidget(uiBinder.createAndBindUi(this));

        radioGroupId++;
        processingTypeCluster.setName("processingType" + radioGroupId);
        processingTypeUser.setName("processingType" + radioGroupId);
        processingTypeUser.setValue(true);

        processingTypeCluster.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                setComponentsEnabled(false);
            }
        });

        processingTypeUser.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                setComponentsEnabled(true);
            }
        });
    }

    private void setComponentsEnabled(boolean enabled) {
        crsText.setEnabled(enabled);
        bandList.setEnabled(enabled);
        quicklooks.setEnabled(enabled);
        outputFormat.setEnabled(enabled);
        autoStaging.setEnabled(enabled);
    }

    public void showProcessingTypeSettings() {
        showProcessingTypeSettings = true;
        processingTypePanel.setVisible(true);
        productRelatedPanel.getElement().getStyle().setProperty("marginLeft", "1.5em");
        tailoringPanel.getElement().getStyle().setProperty("marginLeft", "1.5em");
    }

    public void showProductRelatedSettings() {
        showProductRelatedSettings = true;
        productRelatedPanel.setVisible(true);
    }

    public void showTailoringRelatedSettings() {
        showTailoringRelatedSettings = true;
        tailoringPanel.setVisible(true);
    }

    public void validateForm() throws ValidationException {
        if (showTailoringRelatedSettings && bandList.getSelectedIndex() == -1) {
            throw new ValidationException(bandList, "Output Parameters: One or more bands must be selected.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        String prodName = getProductionName();
        if (!prodName.isEmpty()) {
            parameters.put("productionName", prodName);
        }
        if (showProcessingTypeSettings) {
            if (processingTypeUser.getValue()) {
                parameters.put("outputFormat", getOutputFormat());
                parameters.put("autoStaging", autoStaging.getValue() + "");
                if (showTailoringRelatedSettings) {
                    parameters.put("outputCRS", crsText.getValue());
                    parameters.put("quicklooks", quicklooks.getValue() + "");
                    StringBuilder sb = new StringBuilder();
                    int itemCount = bandList.getItemCount();
                    for (int i = 0; i < itemCount; i++) {
                        if (bandList.isItemSelected(i)) {
                            if (sb.length() != 0) {
                                sb.append(",");
                            }
                            sb.append(bandList.getItemText(i));
                        }
                    }
                    parameters.put("outputBandList", sb.toString());
                }
            } else {
                parameters.put("outputFormat", "SEQ");
            }
        } else if (showProductRelatedSettings) {
            parameters.put("outputFormat", getOutputFormat());
            parameters.put("autoStaging", autoStaging.getValue() + "");
        }


        return parameters;
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

    private String getOutputFormat() {
        int index = outputFormat.getSelectedIndex();
        return outputFormat.getValue(index);
    }

    private String getProductionName() {
        return productionName.getValue().trim();
    }
}