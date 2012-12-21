package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DoubleBox;
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

    @UiField
    TextBox productionName;

    @UiField
    Panel processingFormatPanel;
    @UiField
    RadioButton processingFormatCluster;
    @UiField
    RadioButton processingFormatUser;

    @UiField
    Panel tailoringPanel;
    @UiField
    CheckBox enableTailoring;
    @UiField
    TextArea crsText;
    @UiField
    CheckBox replaceNans;
    @UiField
    DoubleBox replaceValue;
    @UiField
    ListBox bandListBox;
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
        processingFormatCluster.setName("processingFormat" + radioGroupId);
        processingFormatUser.setName("processingFormat" + radioGroupId);
        processingFormatUser.setValue(true);

        processingFormatCluster.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                setComponentsEnabled(false);
            }
        });

        processingFormatUser.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                setComponentsEnabled(true);
            }
        });
        enableTailoring.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                setTailoringComponentsEnabled(event.getValue());
            }
        });
        setTailoringComponentsEnabled(false);
    }

    public void showFormatSelectionPanel(boolean show) {
        if (show) {
            processingFormatPanel.setVisible(show);
            productRelatedPanel.getElement().getStyle().setProperty("marginLeft", "1.5em");
            tailoringPanel.getElement().getStyle().setProperty("marginLeft", "1.5em");
        } else {
            processingFormatUser.setValue(true);
            processingFormatPanel.setVisible(show);
            productRelatedPanel.getElement().getStyle().setProperty("marginLeft", "0em");
            tailoringPanel.getElement().getStyle().setProperty("marginLeft", "0em");
        }
    }

    public void showTailoringRelatedSettings(boolean show) {
        tailoringPanel.setVisible(show);
        enableTailoring.setVisible(show);
    }

    public void validateForm() throws ValidationException {
        if (enableTailoring.getValue() && bandListBox.getSelectedIndex() == -1) {
            throw new ValidationException(bandListBox, "Output Parameters: One or more bands must be selected.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        String prodName = getProductionName();
        if (!prodName.isEmpty()) {
            parameters.put("productionName", prodName);
        }
        if (processingFormatUser.getValue()) {
            parameters.put("outputFormat", getOutputFormat());
            parameters.put("autoStaging", String.valueOf(autoStaging.getValue()));
        } else {
            parameters.put("outputFormat", "SEQ");
            parameters.put("autoStaging", "false");
        }
        if (enableTailoring.getValue()) {
            parameters.put("replaceNans", String.valueOf(replaceNans.getValue()));
            parameters.put("replaceValue", String.valueOf(replaceValue.getValue()));
            parameters.put("outputCRS", crsText.getValue());
            parameters.put("quicklooks", String.valueOf(quicklooks.getValue()));
            parameters.put("outputBandList", createBandListString());
        }


        return parameters;
    }

    private String createBandListString() {
        StringBuilder sb = new StringBuilder();
        int itemCount = bandListBox.getItemCount();
        for (int i = 0; i < itemCount; i++) {
            if (bandListBox.isItemSelected(i)) {
                if (sb.length() != 0) {
                    sb.append(",");
                }
                sb.append(bandListBox.getItemText(i));
            }
        }
        return sb.toString();
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

    private void setComponentsEnabled(boolean enabled) {
        enableTailoring.setEnabled(enabled);
        setTailoringComponentsEnabled(enabled);
        outputFormat.setEnabled(enabled);
        autoStaging.setEnabled(enabled);
    }


    private void setTailoringComponentsEnabled(boolean enabled) {
        boolean enableComponent = enabled && enableTailoring.getValue();
        crsText.setEnabled(enableComponent);
        replaceNans.setEnabled(enableComponent);
        replaceValue.setEnabled(enableComponent);
        bandListBox.setEnabled(enableComponent);
        quicklooks.setEnabled(enableComponent);
    }

    private String getOutputFormat() {
        int index = outputFormat.getSelectedIndex();
        return outputFormat.getValue(index);
    }

    private String getProductionName() {
        return productionName.getValue().trim();
    }
}