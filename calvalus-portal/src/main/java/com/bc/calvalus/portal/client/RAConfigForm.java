/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoRegionDataInfo;
import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.Scheduler;
import com.google.gwt.dom.client.Document;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.event.dom.client.KeyUpEvent;
import com.google.gwt.event.dom.client.KeyUpHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.regexp.shared.RegExp;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Event;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.SelectionChangeEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Form for RA (region analysis) parameters.
 *
 * @author MarcoZ
 */
public class RAConfigForm extends Composite {
    
    private static final String REGION_DATA_DIR = "region_data";
    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private final PortalContext portalContext;
    private final RABandTable bandTable;
    private final ManagedFiles managedFiles;

    interface TheUiBinder extends UiBinder<Widget, RAConfigForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    CalvalusStyle style;
    
    ///
    @UiField
    TextBox steppingPeriodLength;
    @UiField
    TextBox compositingPeriodLength;
    @UiField
    IntegerBox periodsCount;
    @UiField
    ListBox periodsListBox;
    ///
    @UiField(provided = true)
    ListBox regionSourcesList;
    @UiField
    Button addRegionSource;
    @UiField
    Button removeRegionSource;
    @UiField
    ListBox regionSourceAttributeNames;   
    @UiField
    TextBox regionSourceAttributeFilter;
    @UiField
    TextArea selecteRegionSourceAttributes;
    ///
    @UiField
    TextBox goodPixelExpression;
    @UiField
    TextBox percentiles;
    @UiField
    CheckBox writePerRegion;
    @UiField
    CheckBox writeSeparateHistogram;
    @UiField
    CheckBox writePixelValues;
    ///
    @UiField(provided = true)
    CellTable<RABandTable.ConfiguredBand> bandCellTable;
    @UiField
    Button addBandButton;
    @UiField
    Button removeBandButton;
    

    private Date minDate;
    private Date maxDate;
    private String[][] computedPeriods = new String[0][];
    private List<String> inputVarNames;
    private DtoRegionDataInfo regionDataInfo;
    private String restoredRegionSourceAttributeName;

    public RAConfigForm(final PortalContext portalContext) {
        this.portalContext = portalContext;
        this.inputVarNames = new ArrayList<String>();

        bandTable = new RABandTable();
        bandCellTable = bandTable.getCellTable();

        // http://stackoverflow.com/questions/22629632/gwt-listbox-onchangehandler
        // fire selection event even when set programmatically
        regionSourcesList = new ListBox() {
            @Override
            public void setSelectedIndex(int index) {
                super.setSelectedIndex(index);
                DomEvent.fireNativeEvent(Document.get().createChangeEvent(), this);
            }
        };
        
        initWidget(uiBinder.createAndBindUi(this));

        ValueChangeHandler<String> periodCountUpdater = event -> updatePeriodCount();

        steppingPeriodLength.setValue("10");
        steppingPeriodLength.addValueChangeHandler(periodCountUpdater);

        compositingPeriodLength.setValue("10");
        compositingPeriodLength.addValueChangeHandler(periodCountUpdater);

        periodsCount.setText("");
        periodsCount.setEnabled(false);
        periodsListBox.setEnabled(false);
        
        HTML description1 = new HTML("The supported file types are ESRI shapefiles inside a zip. " +
                                             "The zip file name must be identical to the shape file name, " +
                                             "e.g. myregions.shp, myregions.prj, ... packed in myregions.zip without subdirectories.");
        
        managedFiles = new ManagedFiles(portalContext.getBackendService(),
                                        regionSourcesList,
                                        REGION_DATA_DIR,
                                        "Region sources",
                                        description1);
        managedFiles.setAddButton(addRegionSource);
        managedFiles.setRemoveButton(removeRegionSource);
        managedFiles.updateUserFiles(false);

        regionSourcesList.addChangeHandler(event -> loadRegionDataInfo());
        ChangeHandler shapeRegexHandler = event -> updateShapefileFilter();
        regionSourceAttributeNames.addChangeHandler(shapeRegexHandler);
        regionSourceAttributeFilter.addChangeHandler(shapeRegexHandler);
        regionSourceAttributeFilter.addKeyUpHandler(new KeyUpHandler() {
            @Override
            public void onKeyUp(KeyUpEvent event) {
                updateShapefileFilter();   
            }
        });
        enableShapfileDetails(false);

        bandTable.initStyle(style);
        addBandButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                bandTable.addRow();
                removeBandButton.setEnabled(bandTable.getBandList().size() > 0 && bandTable.hasSelection());
            }
        });

        removeBandButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                bandTable.removeSelectedRow();
                removeBandButton.setEnabled(bandTable.getBandList().size() > 0 && bandTable.hasSelection());
            }
        });

        bandTable.addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent event) {
                removeBandButton.setEnabled(bandTable.getBandList().size() > 0 && bandTable.hasSelection());
            }
        });
        removeBandButton.setEnabled(bandTable.getBandList().size() > 0 && bandTable.hasSelection());
    }



    private void loadRegionDataInfo() {
        // load attribute names + values
        enableShapfileDetails(false);
        String selectedFilePath = managedFiles.getSelectedFilePath();
        if (!selectedFilePath.isEmpty()) {
            portalContext.getBackendService().loadRegionDataInfo(selectedFilePath, new AsyncCallback<DtoRegionDataInfo>() {
                @Override
                public void onFailure(Throwable caught) {
                    enableShapfileDetails(true);
                    regionDataInfo = null;
                    restoredRegionSourceAttributeName = null;
                    Dialog.error("Failed", "Failed to load region source information " + selectedFilePath + ": " + caught.getMessage());
                }

                @Override
                public void onSuccess(DtoRegionDataInfo newregionDataInfo) {
                    enableShapfileDetails(true);
                    regionDataInfo = newregionDataInfo;
                    String[] headers = newregionDataInfo.getHeader();
                    regionSourceAttributeNames.clear();
                    for (String header : headers) {
                        regionSourceAttributeNames.addItem(header);
                    }
                    if (restoredRegionSourceAttributeName != null) {
                        int index = Arrays.asList(headers).indexOf(restoredRegionSourceAttributeName);
                        if (index >= 0) {
                            regionSourceAttributeNames.setSelectedIndex(index);
                        }
                        restoredRegionSourceAttributeName = null;
                    }
                    updateShapefileFilter();
                }
            });
        }
    }

    private void enableShapfileDetails(boolean enabled) {
        regionSourceAttributeNames.setEnabled(enabled);
        regionSourceAttributeFilter.setEnabled(enabled);
        selecteRegionSourceAttributes.setEnabled(enabled);
    }

    private void updateShapefileFilter() {
        int selectedIndex = regionSourceAttributeNames.getSelectedIndex();
        if (selectedIndex != -1) {
            String[] values = regionDataInfo.getValues(selectedIndex);
            String filter = regionSourceAttributeFilter.getValue();
            String[] filters = filter.split(",");
            RegExp[] regExps = new RegExp[filters.length];
            for (int i = 0; i < filters.length; i++) {
                regExps[i] = RegExp.compile(filters[i].trim());
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                String value = values[i];
                for (RegExp regExp : regExps) {
                    if (regExp.exec(value) != null) {
                        if (sb.length() > 0) {
                            sb.append(", ");
                        }
                        sb.append(value);
                        break;
                    }
                }
            }
            selecteRegionSourceAttributes.setValue(sb.toString());
        } else {
            selecteRegionSourceAttributes.setValue("");
        }
    }

    public void updateTemporalParameters(Date minDate, Date maxDate) {
        this.minDate = minDate;
        this.maxDate = maxDate;
        updatePeriodCount();
    }

    private void updatePeriodCount() {
        if (minDate != null && maxDate != null) {
            BackendServiceAsync bs = portalContext.getBackendService();
            bs.calculateL3Periods(DATE_FORMAT.format(minDate),
                                  DATE_FORMAT.format(maxDate),
                                  steppingPeriodLength.getValue(),
                                  compositingPeriodLength.getValue(),
                                  new AsyncCallback<String[][]>() {
                                      @Override
                                      public void onFailure(Throwable caught) {
                                          computedPeriods = new String[0][];
                                          updatePeriodsList();
                                      }

                                      @Override
                                      public void onSuccess(String[][] result) {
                                          computedPeriods = result;
                                          updatePeriodsList();
                                      }
                                  });
        } else {
            updatePeriodsList();
        }
    }

    private void updatePeriodsList() {
        periodsListBox.clear();
        for (String[] dates : computedPeriods) {
            periodsListBox.addItem(dates[0] + "    " + dates[1]);
        }
        periodsCount.setValue(computedPeriods.length);
    }
    
    public void setProcessorDescriptor(DtoProcessorDescriptor processor, DtoProductSet productSet) {
        inputVarNames.clear();

        if (processor != null) {
            for (DtoProcessorVariable variable : processor.getProcessorVariables()) {
                inputVarNames.add(variable.getName());
            }
        } else {
            String[] bandNames = productSet.getBandNames();
            Collections.addAll(inputVarNames, bandNames);
        }
        bandTable.setAvailableVariables(inputVarNames);
    }

    public void validateForm() throws ValidationException {
        boolean periodCountValid = computedPeriods.length >= 1;
        if (!periodCountValid) {
            throw new ValidationException(periodsCount, "More than 1 period must be defined.");
        }
        String[] singlePercentiles = percentiles.getValue().split(",");
        for (String singlePercentile : singlePercentiles) {
            if (singlePercentile.trim().isEmpty()) {
                continue;
            }
            int p;
            try {
                p = Integer.parseInt(singlePercentile);
            } catch (NumberFormatException nfe) {
                throw new ValidationException(percentiles, "Percentiles must be integer values: "+ singlePercentile);
            }
            if (p <= 0) {
                throw new ValidationException(percentiles, "Percentiles must be bigger than 0: "+ singlePercentile);
            }
            if (p >= 100) {
                throw new ValidationException(percentiles, "Percentiles must be less than 100: "+ singlePercentile);
            }
        }
        boolean regionSourceSelected = regionSourcesList.getSelectedIndex() != -1;
        if (!regionSourceSelected) {
            throw new ValidationException(periodsCount, "A region source must be selected.");
        }
        List<RABandTable.ConfiguredBand> bandList = bandTable.getBandList();
        for (int i = 0; i < bandList.size(); i++) {
            if (bandList.get(i).getName().trim().isEmpty()) {
                throw new ValidationException(bandCellTable, "Statistics band number " + (i + 1) + ": Name must not be emtpy.");
            }
        }
        if (bandList.size() == 0) {
            throw new ValidationException(bandCellTable, "At least one statistics band must be defined.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<>();
        
        parameters.put("periodLength", steppingPeriodLength.getValue());
        parameters.put("compositingPeriodLength", compositingPeriodLength.getValue());

        parameters.put("regionSource", regionSourcesList.getSelectedValue());
        parameters.put("regionSourceAttributeName", regionSourceAttributeNames.getSelectedValue());
        parameters.put("regionSourceAttributeFilter", regionSourceAttributeFilter.getValue());
        
        parameters.put("goodPixelExpression", ParametersEditorGenerator.encodeXML(goodPixelExpression.getText()));
        parameters.put("percentiles", percentiles.getText());
        parameters.put("writePerRegion", writePerRegion.getValue().toString());
        parameters.put("writeSeparateHistogram", writeSeparateHistogram.getValue().toString());
        parameters.put("writePixelValues", writePixelValues.getValue().toString());

        List<RABandTable.ConfiguredBand> bandList = bandTable.getBandList();
        for (int i = 0; i < bandList.size(); i++) {
            RABandTable.ConfiguredBand configuredBand = bandList.get(i);
            parameters.put("statband." + i + ".name", configuredBand.getName());
            if (configuredBand.hasHistogram()) {
                parameters.put("statband." + i + ".numBins", configuredBand.getNumBins());    
                parameters.put("statband." + i + ".min", configuredBand.getMin());    
                parameters.put("statband." + i + ".max", configuredBand.getMax());    
            }
        }
        parameters.put("statband.count", ""+bandList.size());

        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        steppingPeriodLength.setValue(parameters.getOrDefault("periodLength", "10"), true);
        compositingPeriodLength.setValue(parameters.getOrDefault("compositingPeriodLength", "10"), true);

        String regionSource = parameters.get("regionSource");
        if (regionSource != null && !regionSource.isEmpty()) {
            for (int i = 0; i < regionSourcesList.getItemCount(); i++) {
                if (regionSourcesList.getValue(i).equals(regionSource)) {
                    restoredRegionSourceAttributeName = parameters.get("regionSourceAttributeName");
                    regionSourceAttributeFilter.setValue(parameters.getOrDefault("regionSourceAttributeFilter", ""));
                    regionSourcesList.setSelectedIndex(i);
                }
            }
        }
        
        goodPixelExpression.setValue(ParametersEditorGenerator.decodeXML(parameters.getOrDefault("goodPixelExpression", "")));
        percentiles.setValue(parameters.getOrDefault("percentiles", ""));
        writePerRegion.setValue(Boolean.valueOf(parameters.getOrDefault("writePerRegion", "true")));
        writeSeparateHistogram.setValue(Boolean.valueOf(parameters.getOrDefault("writeSeparateHistogram", "true")));
        writePixelValues.setValue(Boolean.valueOf(parameters.getOrDefault("writePixelValues", "false")));

        int bandCount = Integer.parseInt(parameters.getOrDefault("statband.count", "0"));
        List<RABandTable.ConfiguredBand> bandList = bandTable.getBandList();
        bandList.clear();
        for (int i = 0; i < bandCount; i++) {
            String bandName = parameters.get("statband." + i + ".name");
            String numBins = parameters.get("statband." + i + ".numBins");
            String min = parameters.get("statband." + i + ".min");
            String max = parameters.get("statband." + i + ".max");
            RABandTable.ConfiguredBand configuredBand;
            if (numBins != null && min != null && max != null) {
                configuredBand = new RABandTable.ConfiguredBand(bandName, true, numBins, min, max);
            } else {
                configuredBand = new RABandTable.ConfiguredBand(bandName);
            }
            bandList.add(configuredBand);
        }
        bandTable.refresh();
    }
    
    // fires change event after each key press
    // http://stackoverflow.com/questions/3184648/instant-value-change-handler-on-a-gwt-textbox
    private static class InstantTextBox extends TextBox {
    
        public InstantTextBox() {
            super();
            sinkEvents(Event.ONPASTE);
        }
    
        @Override
        public void onBrowserEvent(Event event) {
            super.onBrowserEvent(event);
            switch (DOM.eventGetType(event)) {
                case Event.ONPASTE:
                    Scheduler.get().scheduleDeferred(() -> ValueChangeEvent.fire(InstantTextBox.this, getText()));
                    break;
            }
        }
    }
}
