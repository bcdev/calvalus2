/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.IntegerBox;
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
    DoubleBox replaceNanValue;
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

    @UiField
    IntegerBox allowedFailure;

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
        allowedFailure.setValue(0);
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
        if (enableTailoring.getValue()) {
            if (replaceNanValue.getValue() == null) {
                throw new ValidationException(replaceNanValue, "Value for replacing NaN must be given.");
            }
            if (bandListBox.getSelectedIndex() == -1) {
                throw new ValidationException(bandListBox, "Output Parameters: One or more bands must be selected.");
            }
            if ("Multi-GeoTIFF".equals(getOutputFormat()) && crsText.getValue().trim().isEmpty()) {
                throw new ValidationException(crsText,
                                              "Output Parameters: If Multi-GeoTIFF is selected as output format " +
                                              "also a CRS must be specified.");
            }
        }
        Integer allowedFailureValue = allowedFailure.getValue();
        boolean allowedFailureValid = allowedFailureValue != null
                && allowedFailureValue >= 0
                && allowedFailureValue <= 100;
        if (!allowedFailureValid) {
            throw new ValidationException(allowedFailure, "Allowed failures must be an integer between 0 and 100");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        String prodName = getProductionName();
        if (!prodName.isEmpty()) {
            parameters.put("productionName", prodName);
        }
        if (processingFormatUser.getValue()) {
            String outputFormat = getOutputFormat();
            if (outputFormat != null) {
                parameters.put("outputFormat", outputFormat);
            }
            parameters.put("autoStaging", String.valueOf(autoStaging.getValue()));
        } else {
            parameters.put("outputFormat", "SEQ");
            parameters.put("autoStaging", "false");
        }
        if (enableTailoring.getValue()) {
            if (replaceNans.getValue()) {
                parameters.put("replaceNanValue", replaceNanValue.getValue().toString());
            }
            parameters.put("outputCRS", crsText.getValue());
            parameters.put("quicklooks", quicklooks.getValue().toString());
            parameters.put("outputBandList", createBandListString());
        }
        parameters.put("calvalus.hadoop.mapreduce.map.failures.maxpercent", allowedFailure.getValue().toString());
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

    public void setValues(Map<String, String> parameters) {
        String productionNameValue = parameters.get("productionName");
        if (productionNameValue != null) {
            productionName.setValue(productionNameValue);
        }
        String outputFormatValue = parameters.get("outputFormat");
        if (outputFormatValue != null) {
            if (outputFormatValue.equals("SEQ")) {
                autoStaging.setValue(false);
                processingFormatCluster.setValue(true, true);
            } else {
                outputFormat.setSelectedIndex(0);
                for (int i = 0; i < outputFormat.getItemCount(); i++) {
                    if (outputFormat.getItemText(i).equals(outputFormatValue)) {
                        outputFormat.setSelectedIndex(i);
                        break;
                    }
                }
            }
        } else {
            outputFormat.setSelectedIndex(0);
            processingFormatUser.setValue(true, true);
        }
        String autoStagingValue = parameters.get("autoStaging");
        if (autoStagingValue != null) {
            autoStaging.setValue(Boolean.valueOf(autoStagingValue));
        }
        String allowedFailureValue = parameters.get("calvalus.hadoop.mapreduce.map.failures.maxpercent");
        if (allowedFailureValue != null) {
            allowedFailure.setValue(Integer.valueOf(allowedFailureValue));
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
        replaceNanValue.setEnabled(enableComponent);
        bandListBox.setEnabled(enableComponent);
        quicklooks.setEnabled(enableComponent);
    }

    private String getOutputFormat() {
        int index = outputFormat.getSelectedIndex();
        if (index > -1) {
            return outputFormat.getValue(index);
        } else {
            return null;
        }
    }

    private String getProductionName() {
        return productionName.getValue().trim();
    }
}