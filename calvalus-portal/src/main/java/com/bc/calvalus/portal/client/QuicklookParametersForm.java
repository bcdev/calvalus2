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

import com.bc.calvalus.portal.shared.DtoColorPalette;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Element;
import com.google.gwt.dom.client.Style;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTMLPanel;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.Label;
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

    private static int instanceCounter = 0;
    private int radioGroupId = 0;

    private static final String CUSTOM_BAND = "<custom>";

    private static final String BANDNAME_LISTBOX_ROW = "bandNameListBoxRow";

    private final PortalContext portal;

    @UiField
    HTMLPanel singleBandPanel;
    @UiField
    HTMLPanel multiBandPanel;
    @UiField
    HTMLPanel moreOptionsPanel;

    @UiField
    RadioButton quicklookNone;
    @UiField
    RadioButton quicklookSingleBand;
    @UiField
    RadioButton quicklookMultiBand;

    @UiField
    ListBox bandNameListBox;
    @UiField
    Label bandNameRowLabel;
    @UiField
    TextBox bandName;
    @UiField
    ListBox colorPalette;

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

    private DtoColorPalette[] availableColorPalettes = null;
    private Boolean pageLoaded = false;
    private String[] availableBandNames = null;

    public QuicklookParametersForm(PortalContext portalContext) {
        this.portal = portalContext;

        initWidget(uiBinder.createAndBindUi(this));

        instanceCounter++;
        radioGroupId = instanceCounter;

        //give radio group a unique name for each instance
        quicklookNone.setName("quicklookSelection" + radioGroupId);
        quicklookSingleBand.setName("quicklookSelection" + radioGroupId);
        quicklookMultiBand.setName("quicklookSelection" + radioGroupId);

        quicklookNone.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                if (event.getValue())
                    quicklookNoneChangeHandler();
            }
        });

        quicklookSingleBand.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                if (event.getValue())
                    quicklookSingleBandChangeHandler();
            }
        });

        quicklookMultiBand.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                if (event.getValue())
                    quicklookMultiBandChangeHandler();
            }
        });

        bandName.addValueChangeHandler(new ValueChangeHandler<String>() {
            @Override
            public void onValueChange(ValueChangeEvent<String> event) {
                bandNameChangeHandler();
            }
        });

        bandNameListBox.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                bandNameListBoxChangeHandler();
            }
        });

        setAvailableImageTypes();
        setColorPalettes();
    }

    @Override
    protected void onLoad() {
        //give HTML element a unique id for each instance
        Element element = DOM.getElementById(BANDNAME_LISTBOX_ROW);
        element.setId(BANDNAME_LISTBOX_ROW + radioGroupId);

        this.pageLoaded = true;
        quicklookNone.setValue(true, true);
        buildBandNameListBox();
    }

    private void bandNameChangeHandler() {
        // band name field changed, reset list box index and call it's handler
        int itemCount = bandNameListBox.getItemCount();
        String bandNameValue = bandName.getValue();
        bandNameListBox.setSelectedIndex(0);
        for (int i = 0; i < itemCount; i++) {
            String listBand = bandNameListBox.getValue(i);
            if (listBand != null && listBand.equals(bandNameValue)) {
                bandNameListBox.setSelectedIndex(i);
                break;
            }
        }
        bandNameListBoxChangeHandler();
    }

    private void bandNameListBoxChangeHandler() {
        if (!pageLoaded)
            return;

        int selectedIndex = bandNameListBox.getSelectedIndex();
        Element element = DOM.getElementById(BANDNAME_LISTBOX_ROW + radioGroupId);
        if (!quicklookSingleBand.getValue() || selectedIndex < 0) {
            // single band not selected OR no values in listbox
            // enable bandName textbox, make drop down list invisible
            bandNameRowLabel.setText("Band name:");
            bandName.setEnabled(true);
            if (element != null)
                element.getStyle().setDisplay(Style.Display.NONE);
        } else {
            // single band is selected AND values in listbox
            // make drop down list visible
            if (element != null)
                element.getStyle().setDisplay(Style.Display.TABLE_ROW);
            bandNameRowLabel.setText("");
            if (selectedIndex == 0) {
                // custom band name option selected
                // enable bandName textbox
                bandName.setEnabled(true);
            } else if (selectedIndex > 0) {
                // band name (sourced from processor) selected
                // disable bandName textbox
                bandName.setValue(bandNameListBox.getValue(selectedIndex));
                bandName.setEnabled(false);
            }
        }
    }

    private void quicklookNoneChangeHandler() {
        singleBandPanel.setVisible(false);
        multiBandPanel.setVisible(false);
        moreOptionsPanel.setVisible(false);
    }

    private void quicklookSingleBandChangeHandler() {
        singleBandPanel.setVisible(true);
        multiBandPanel.setVisible(false);
        moreOptionsPanel.setVisible(true);
        bandNameChangeHandler();
    }

    private void quicklookMultiBandChangeHandler() {
        singleBandPanel.setVisible(false);
        multiBandPanel.setVisible(true);
        moreOptionsPanel.setVisible(true);
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

    private void setColorPalettes() {
        this.availableColorPalettes = portal.getColorPalettes();
        colorPalette.clear();
        colorPalette.addItem("");
        for (DtoColorPalette dtoColorPalette : this.availableColorPalettes) {
            colorPalette.addItem(dtoColorPalette.getQualifiedName(), dtoColorPalette.getCpdURL());
        }
        colorPalette.setSelectedIndex(0);
    }

    public void setBandNames(String... bandNames) {
        this.availableBandNames = bandNames;
        buildBandNameListBox();
    }

    private void buildBandNameListBox() {
        bandNameListBox.clear();
        if (!pageLoaded)
            return;

        if (availableBandNames != null && availableBandNames.length > 0) {
            bandNameListBox.addItem(CUSTOM_BAND);
            bandNameListBox.setSelectedIndex(0);
            for (String bandName : this.availableBandNames) {
                bandNameListBox.addItem(bandName);
            }
        }
        bandNameChangeHandler();
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
        if (legendEnabledValue) {
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

            // color palette
            String cpdURLXML = "";
            String colorPaletteValue = colorPalette.getSelectedValue();
            if (colorPaletteValue != null && !colorPaletteValue.isEmpty()) {
                cpdURLXML = indentXML + "<cpdURL>" + colorPaletteValue + "</cpdURL>\n";
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


            bandName.setValue(null);
            colorPalette.setSelectedIndex(0);

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

            // set no quicklook radio button in GUI
            quicklookNone.setValue(true, true);
        } else {
            Document dom = XMLParser.parse(quicklookParametersValue);

            // bandName 
            String bandNameValue = getTagValue(dom, "bandName");
            if (bandNameValue != null) {
                bandName.setValue(bandNameValue);

                // As bandName has a value, assume this is a single band
                // Therefore, set single band radio button in GUI
                quicklookSingleBand.setValue(true, true);
            }

            // color palette
            String cpdURLValue = getTagValue(dom, "cpdURL");
            if (cpdURLValue == null || this.availableColorPalettes == null) {
                colorPalette.setSelectedIndex(0);
            } else {
                for (int i = 0; i < this.availableColorPalettes.length; i++) {
                    if (this.availableColorPalettes[i].getCpdURL().equals(cpdURLValue)) {
                        colorPalette.setSelectedIndex(i + 1);
                        break;
                    }
                }
            }

            // RGBAExpressions
            String RGBAExpressionsValue = getTagValue(dom, "RGBAExpressions");
            if (RGBAExpressionsValue != null) {
                String[] tokens = RGBAExpressionsValue.split(",");
                int numTokens = tokens.length;
                rgbaExpressionsRedBand.setValue(numTokens > 0 ? tokens[0] : null);
                rgbaExpressionsGreenBand.setValue(numTokens > 1 ? tokens[1] : null);
                rgbaExpressionsBlueBand.setValue(numTokens > 2 ? tokens[2] : null);
                rgbaExpressionsAlphaBand.setValue(numTokens > 3 ? tokens[3] : null);

                // As RGBAExpressions has a value, assume this is a multi band,
                // Therefore, set multi band radio button in GUI
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

