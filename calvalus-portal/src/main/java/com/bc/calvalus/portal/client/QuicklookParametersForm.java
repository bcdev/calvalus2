/*
 * Copyright (C) 2018 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Node;
import com.google.gwt.xml.client.NodeList;
import com.google.gwt.xml.client.XMLParser;

import java.util.HashMap;
import java.util.Map;

/**
 * This form is used regarding visualisations settings.
 *
 * @author Declan
 */
public class QuicklookParametersForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, QuicklookParametersForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    RadioButton quicklookNone;
    @UiField
    RadioButton quicklookSingleBand;
    @UiField
    RadioButton quicklookMultiBand;

    @UiField
    TextBox bandName;
    @UiField
    TextBox cpdURL;

    @UiField
    TextBox rgbaExpressionsRedBand;
    @UiField
    TextBox rgbaExpressionsGreenBand;
    @UiField
    TextBox rgbaExpressionsBlueBand;
    @UiField
    TextBox rgbaExpressionsAlphaBand;
    @UiField
    TextBox rgbaMinSamples;
    @UiField
    TextBox rgbaMaxSamples;

    @UiField
    ListBox imageType;
    @UiField
    TextBox overlayURL;
    @UiField
    TextBox maskOverlays;
    @UiField
    IntegerBox subSamplingX;
    @UiField
    IntegerBox subSamplingY;
    @UiField
    TextBox backgroundColor;
    @UiField
    CheckBox legendEnabled;

    public QuicklookParametersForm(PortalContext portalContext) {
        initWidget(uiBinder.createAndBindUi(this));

        quicklookNone.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                setQuicklookNone();
            }
        });

        quicklookSingleBand.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                setQuicklookSingleBandEnabled();
            }
        });

        quicklookMultiBand.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                setQuicklookMultiBandEnabled();
            }
        });

        setAvailableImageTypes();
        quicklookNone.setValue(true);
        setQuicklookNone();
    }

    private void setQuicklookNone() {
        bandName.setEnabled(false);
        cpdURL.setEnabled(false);

        rgbaExpressionsRedBand.setEnabled(false);
        rgbaExpressionsGreenBand.setEnabled(false);
        rgbaExpressionsBlueBand.setEnabled(false);
        rgbaExpressionsAlphaBand.setEnabled(false);
        rgbaMinSamples.setEnabled(false);
        rgbaMaxSamples.setEnabled(false);

        imageType.setEnabled(false);
        overlayURL.setEnabled(false);
        maskOverlays.setEnabled(false);
        subSamplingX.setEnabled(false);
        subSamplingY.setEnabled(false);
        backgroundColor.setEnabled(false);
        legendEnabled.setEnabled(false);
    }

    private void setQuicklookSingleBandEnabled() {
        bandName.setEnabled(true);
        cpdURL.setEnabled(true);

        rgbaExpressionsRedBand.setEnabled(false);
        rgbaExpressionsGreenBand.setEnabled(false);
        rgbaExpressionsBlueBand.setEnabled(false);
        rgbaExpressionsAlphaBand.setEnabled(false);
        rgbaMinSamples.setEnabled(false);
        rgbaMaxSamples.setEnabled(false);

        imageType.setEnabled(true);
        overlayURL.setEnabled(true);
        maskOverlays.setEnabled(true);
        subSamplingX.setEnabled(true);
        subSamplingY.setEnabled(true);
        backgroundColor.setEnabled(true);
        legendEnabled.setEnabled(true);
    }

    private void setQuicklookMultiBandEnabled() {
        bandName.setEnabled(false);
        cpdURL.setEnabled(false);

        rgbaExpressionsRedBand.setEnabled(true);
        rgbaExpressionsGreenBand.setEnabled(true);
        rgbaExpressionsBlueBand.setEnabled(true);
        rgbaExpressionsAlphaBand.setEnabled(true);
        rgbaMinSamples.setEnabled(true);
        rgbaMaxSamples.setEnabled(true);

        imageType.setEnabled(true);
        overlayURL.setEnabled(true);
        maskOverlays.setEnabled(true);
        subSamplingX.setEnabled(true);
        subSamplingY.setEnabled(true);
        backgroundColor.setEnabled(true);
        legendEnabled.setEnabled(true);
    }

    public void setAvailableImageTypes() {
        String[] imageNames = {"jpeg", "tiff", "png"};
        int selectedIndex = imageType.getSelectedIndex();
        imageType.clear();
        for (String imageName : imageNames) {
            imageType.addItem(imageName);
        }
        if (selectedIndex >= 0 && selectedIndex < imageNames.length) {
            imageType.setSelectedIndex(selectedIndex);
        } else {
            imageType.setSelectedIndex(0);
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        if (quicklookNone.getValue()) {
            return parameters;
        }

        String indentXML = "      ";

        // imageType
        String imageTypeXML = indentXML + "<imageType>" + imageType.getSelectedValue() + "</imageType>\n";

        // overlayURL
        String overlayURLValue = overlayURL.getValue();
        String overlayURLXML = "";
        if (overlayURLValue != null && !overlayURLValue.isEmpty()) {
            overlayURLXML = indentXML + "<overlayURL>" + overlayURLValue + "</overlayURL>\n";
        }

        // maskOverlays
        String maskOverlaysValue = maskOverlays.getValue();
        String maskOverlaysXML = "";
        if (maskOverlaysValue != null && !maskOverlaysValue.isEmpty()) {
            maskOverlaysXML = indentXML + "<maskOverlays>" + maskOverlaysValue + "</maskOverlays>\n";
        }

        // subSamplingX
        Integer subSamplingXValue = subSamplingX.getValue();
        String subSamplingXXML = "";
        if (subSamplingXValue != null && subSamplingXValue >= 1) {
            subSamplingXXML = indentXML + "<subSamplingX>" + subSamplingXValue + "</subSamplingX>\n";
        }

        // subSamplingY
        Integer subSamplingYValue = subSamplingY.getValue();
        String subSamplingYXML = "";
        if (subSamplingYValue != null && subSamplingYValue >= 1) {
            subSamplingYXML = indentXML + "<subSamplingY>" + subSamplingYValue + "</subSamplingY>\n";
        }

        // backgroundColor
        String backgroundColorValue = backgroundColor.getValue();
        String backgroundColorXML = "";
        if (backgroundColorValue != null && !backgroundColorValue.isEmpty()) {
            backgroundColorXML = indentXML + "<backgroundColor>" + backgroundColorValue + "</backgroundColor>\n";
        }

        // legendEnabled
        Boolean legendEnabledValue = legendEnabled.getValue();
        String legendEnabledXML = "";
        if (legendEnabled.getValue()) {
            legendEnabledXML = indentXML + "<legendEnabled>true</legendEnabled>\n";
        }

        String quicklookParameters = "";
        if (quicklookSingleBand.getValue()) {

            // bandName
            String bandNameXML = "";
            String bandNameValue = bandName.getValue();
            if (bandNameValue != null && !bandNameValue.isEmpty()) {
                bandNameXML = indentXML + "<bandName>" + bandNameValue + "</bandName>\n";
            }

            // cpdURL
            String cpdURLXML = "";
            String cpdURLValue = cpdURL.getValue();
            if (cpdURLValue != null && !cpdURLValue.isEmpty()) {
                cpdURLXML = indentXML + "<cpdURL>" + cpdURLValue + "</cpdURL>\n";
            }

            quicklookParameters = "<parameters>\n" +
                    "  <quicklooks>\n" +
                    "    <config>\n" +
                    bandNameXML +
                    cpdURLXML +
                    imageTypeXML +
                    overlayURLXML +
                    maskOverlaysXML +
                    subSamplingXXML +
                    subSamplingYXML +
                    backgroundColorXML +
                    legendEnabledXML +
                    "    </config>\n" +
                    "  </quicklooks>\n" +
                    "</parameters>";
        } else if (quicklookMultiBand.getValue()) {

            // RGBAExpressions
            String rgbaExpressionsXML = "";
            String rgbaExpressionsRedBandValue = rgbaExpressionsRedBand.getValue();
            String rgbaExpressionsGreenBandValue = rgbaExpressionsGreenBand.getValue();
            String rgbaExpressionsBlueBandValue = rgbaExpressionsBlueBand.getValue();
            String rgbaExpressionsAlphaBandValue = rgbaExpressionsAlphaBand.getValue();

            if (rgbaExpressionsRedBandValue != null && !rgbaExpressionsRedBandValue.isEmpty() &&
                    rgbaExpressionsGreenBandValue != null && !rgbaExpressionsGreenBandValue.isEmpty() &&
                    rgbaExpressionsBlueBandValue != null && !rgbaExpressionsBlueBandValue.isEmpty()) {

                rgbaExpressionsXML = indentXML + "<RGBAExpressions>" +
                        rgbaExpressionsRedBandValue + "," +
                        rgbaExpressionsGreenBandValue + "," +
                        rgbaExpressionsBlueBandValue;

                if (rgbaExpressionsAlphaBandValue != null && !rgbaExpressionsAlphaBandValue.isEmpty())
                    rgbaExpressionsXML = rgbaExpressionsXML + "," + rgbaExpressionsAlphaBandValue;

                rgbaExpressionsXML = rgbaExpressionsXML + "</RGBAExpressions>\n";
            }

            // rgbaMinSamples
            String rgbaMinSamplesXML = "";
            String rgbaMinSamplesValue = rgbaMinSamples.getValue();
            if (rgbaMinSamplesValue != null && !rgbaMinSamplesValue.isEmpty()) {
                rgbaMinSamplesXML = indentXML + "<rgbaMinSamples>" + rgbaMinSamplesValue + "</rgbaMinSamples>\n";
            }

            // rgbaMaxSamples
            String rgbaMaxSamplesXML = "";
            String rgbaMaxSamplesValue = rgbaMaxSamples.getValue();
            if (rgbaMaxSamplesValue != null && !rgbaMaxSamplesValue.isEmpty()) {
                rgbaMaxSamplesXML = indentXML + "<rgbaMaxSamples>" + rgbaMaxSamplesValue + "</rgbaMaxSamples>\n";
            }

            quicklookParameters = "<parameters>\n" +
                    "  <quicklooks>\n" +
                    "    <config>\n" +
                    rgbaExpressionsXML +
                    rgbaMinSamplesXML +
                    rgbaMaxSamplesXML +
                    imageTypeXML +
                    overlayURLXML +
                    maskOverlaysXML +
                    subSamplingXXML +
                    subSamplingYXML +
                    backgroundColorXML +
                    legendEnabledXML +
                    "    </config>\n" +
                    "  </quicklooks>\n" +
                    "</parameters>";
        }

        if (!quicklookParameters.isEmpty()) {
            parameters.put("quicklooks", "true");
            parameters.put("calvalus.ql.parameters", quicklookParameters);
        }

        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        String quicklookParametersValue = parameters.get("calvalus.ql.parameters");
        if (quicklookParametersValue == null || quicklookParametersValue.isEmpty()) {
            quicklookNone.setValue(true, true);

            bandName.setValue(null);
            cpdURL.setValue(null);

            rgbaExpressionsRedBand.setValue(null);
            rgbaExpressionsGreenBand.setValue(null);
            rgbaExpressionsBlueBand.setValue(null);
            rgbaExpressionsAlphaBand.setValue(null);
            rgbaMinSamples.setValue(null);
            rgbaMaxSamples.setValue(null);

            imageType.setSelectedIndex(0);
            overlayURL.setValue(null);
            maskOverlays.setValue(null);
            subSamplingX.setValue(null);
            maskOverlays.setValue(null);
            legendEnabled.setValue(false);
        } else {
            Document dom = XMLParser.parse(quicklookParametersValue);

            // bandName
            String bandNameValue = getTagValue(dom, "bandName");
            if (bandNameValue != null)
                quicklookSingleBand.setValue(true, true);
            bandName.setValue(bandNameValue);

            // cpdURL
            String cpdURLValue = getTagValue(dom, "cpdURL");
            cpdURL.setValue(cpdURLValue);

            // RGBAExpressions
            String RGBAExpressionsValue = getTagValue(dom, "RGBAExpressions");
            if (RGBAExpressionsValue != null) {
                String[] tokens = RGBAExpressionsValue.split(",");
                int numTokens = tokens.length;
                rgbaExpressionsRedBand.setValue(numTokens > 0 ? tokens[0] : null);
                rgbaExpressionsGreenBand.setValue(numTokens > 1 ? tokens[1] : null);
                rgbaExpressionsBlueBand.setValue(numTokens > 2 ? tokens[2] : null);
                rgbaExpressionsAlphaBand.setValue(numTokens > 3 ? tokens[3] : null);
                quicklookMultiBand.setValue(true, true);
            } else {
                rgbaExpressionsRedBand.setValue(null);
                rgbaExpressionsGreenBand.setValue(null);
                rgbaExpressionsBlueBand.setValue(null);
                rgbaExpressionsAlphaBand.setValue(null);
            }

            // rgbaMinSamples
            String rgbaMinSamplesValue = getTagValue(dom, "rgbaMinSamples");
            rgbaMinSamples.setValue(rgbaMinSamplesValue);

            // rgbaMaxSamples
            String rgbaMaxSamplesValue = getTagValue(dom, "rgbaMaxSamples");
            rgbaMaxSamples.setValue(rgbaMaxSamplesValue);

            // imageType
            String imageTypeValue = getTagValue(dom, "imageType");
            if (imageTypeValue == null)
                imageType.setSelectedIndex(0);
            else if (imageTypeValue.equalsIgnoreCase("jpeg"))
                imageType.setSelectedIndex(0);
            else if (imageTypeValue.equalsIgnoreCase("tiff"))
                imageType.setSelectedIndex(1);
            else if (imageTypeValue.equalsIgnoreCase("png"))
                imageType.setSelectedIndex(2);

            // overlayURL
            String overlayURLValue = getTagValue(dom, "overlayURL");
            overlayURL.setValue(overlayURLValue);

            // maskOverlays
            String maskOverlaysValue = getTagValue(dom, "maskOverlays");
            maskOverlays.setValue(maskOverlaysValue);

            // subSamplingX
            String subSamplingXValue = getTagValue(dom, "subSamplingX");
            try {
                Integer subSamplingXIntValue = Integer.valueOf(subSamplingXValue);
                subSamplingX.setValue(subSamplingXIntValue);
            } catch (NumberFormatException e) {
                subSamplingX.setValue(null);
            }

            // subSamplingY
            String subSamplingYValue = getTagValue(dom, "subSamplingY");
            try {
                Integer subSamplingYIntValue = Integer.valueOf(subSamplingYValue);
                subSamplingY.setValue(subSamplingYIntValue);
            } catch (NumberFormatException e) {
                subSamplingY.setValue(null);
            }

            // backgroundColor
            String backgroundColorValue = getTagValue(dom, "backgroundColor");
            backgroundColor.setValue(backgroundColorValue);

            // legendEnabled
            String legendEnabledValue = getTagValue(dom, "legendEnabled");
            if (legendEnabledValue != null && legendEnabledValue.equalsIgnoreCase("true"))
                legendEnabled.setValue(true);
            else
                legendEnabled.setValue(false);
        }
    }

    private String getTagValue(Document dom, String tagName) {
        NodeList nodeList = dom.getElementsByTagName(tagName);
        if (nodeList != null && nodeList.getLength() > 0) {
            Node node = nodeList.item(0);
            if (node != null && node.hasChildNodes()) {
                Node firstChild = node.getFirstChild();
                if (firstChild != null) {
                    return (firstChild.getNodeValue());
                }
            }
        }
        return null;
    }
}