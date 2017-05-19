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
import com.bc.calvalus.portal.shared.DtoShapefileDetails;
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
    
    private static final String SHAPE_FILE_DIR = "shapefiles";
    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private final PortalContext portalContext;
    private final RABandTable bandTable;
    private final UserManagedFiles userManagedContent;

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
    ListBox shapefileList;
    @UiField
    Button addShapefiles;
    @UiField
    Button removeShapefiles;
    @UiField
    ListBox shapefileAttributeNameList;   
    @UiField
    TextBox shapefileAttributeFilter;
    @UiField
    TextArea shapefileSelectedAttributeValues;
    ///
    @UiField
    TextBox maskExpr;
    @UiField
    TextBox percentiles;
    @UiField
    CheckBox filePerRegion;
    @UiField
    CheckBox histoSeparate;
    @UiField
    CheckBox pixelValues;
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
    private DtoShapefileDetails shapefileDetails;

    public RAConfigForm(final PortalContext portalContext) {
        this.portalContext = portalContext;
        this.inputVarNames = new ArrayList<String>();

        bandTable = new RABandTable();
        bandCellTable = bandTable.getCellTable();

        // http://stackoverflow.com/questions/22629632/gwt-listbox-onchangehandler
        // fire selection event even when set programmatically
        shapefileList = new ListBox() {
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
        
        HTML description1 = new HTML("The supported file types are TAB-separated CSV (<b>*.txt</b>, <b>*.csv</b>)<br/>" +
                                             "and SNAP placemark files (<b>*.placemark</b>).");
        
        
        userManagedContent = new UserManagedFiles(portalContext.getBackendService(),
                                                  shapefileList,
                                                  SHAPE_FILE_DIR,
                                                  "ESRI shapefiles",
                                                  description1);
        addShapefiles.addClickHandler(userManagedContent.getAddAction());
        removeShapefiles.addClickHandler(userManagedContent.getRemoveAction());
        userManagedContent.updateList();

        shapefileList.addChangeHandler(event -> loadShapefileDetails());
        ChangeHandler shapeRegexHandler = event -> updateShapefileFilter();
        shapefileAttributeNameList.addChangeHandler(shapeRegexHandler);
        shapefileAttributeFilter.addChangeHandler(shapeRegexHandler);
        shapefileAttributeFilter.addKeyUpHandler(new KeyUpHandler() {
            @Override
            public void onKeyUp(KeyUpEvent event) {
                updateShapefileFilter();   
            }
        });

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



    private void loadShapefileDetails() {
        // load attribute names + values
        String selectedFilePath = userManagedContent.getSelectedFilePath();
        if (!selectedFilePath.isEmpty()) {
            enableShapfileDetails(false);
            portalContext.getBackendService().loadShapefileDetails(selectedFilePath, new AsyncCallback<DtoShapefileDetails>() {
                @Override
                public void onFailure(Throwable caught) {
                    shapefileDetails = null;
                    shapefileSelectedAttributeValues.setValue(caught.getMessage());
                    enableShapfileDetails(true);
                }

                @Override
                public void onSuccess(DtoShapefileDetails result) {
                    enableShapfileDetails(true);
                    shapefileDetails = result;
                    String[] headers = result.getHeader();
                    shapefileAttributeNameList.clear();
                    for (String header : headers) {
                        shapefileAttributeNameList.addItem(header);
                    }
                    updateShapefileFilter();
                }
            });
        }
    }

    private void enableShapfileDetails(boolean enabled) {
        shapefileAttributeNameList.setEnabled(enabled);
        shapefileAttributeFilter.setEnabled(enabled);
        shapefileSelectedAttributeValues.setEnabled(enabled);
    }

    private void updateShapefileFilter() {
        int selectedIndex = shapefileAttributeNameList.getSelectedIndex();
        if (selectedIndex != -1) {
            String[] values = shapefileDetails.getValues(selectedIndex);
            String filter = shapefileAttributeFilter.getValue();
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
            shapefileSelectedAttributeValues.setValue(sb.toString());
        } else {
            shapefileSelectedAttributeValues.setValue("");
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
//        if (bandListBox.getSelectedIndex() == -1 && bandNames.getText().trim().isEmpty()) {
//            throw new ValidationException(bandListBox, "One or more bands must be selected or entered.");
//        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("maskExpr", maskExpr.getText());
        parameters.put("periodLength", steppingPeriodLength.getValue());
        parameters.put("compositingPeriodLength", compositingPeriodLength.getValue());
//        parameters.put("bandList", createBandListString());
        return parameters;
    }

//    private String createBandListString() {
//        if (bandListBox.getSelectedIndex() != -1) {
//            StringBuilder sb = new StringBuilder();
//            int itemCount = bandListBox.getItemCount();
//            for (int i = 0; i < itemCount; i++) {
//                if (bandListBox.isItemSelected(i)) {
//                    if (sb.length() != 0) {
//                        sb.append(",");
//                    }
//                    sb.append(bandListBox.getItemText(i));
//                }
//            }
//            return sb.toString();
//        } else {
//            return bandNames.getText().trim();
//        }
//    }

    public void setValues(Map<String, String> parameters) {
        maskExpr.setValue(parameters.getOrDefault("maskExpr", ""));
        String periodLengthValue = parameters.get("periodLength");
        if (periodLengthValue != null) {
            steppingPeriodLength.setValue(periodLengthValue, true);
        }
        String compositingPeriodLengthValue = parameters.get("compositingPeriodLength");
        if (compositingPeriodLengthValue != null) {
            compositingPeriodLength.setValue(compositingPeriodLengthValue, true);
        }
//        String bandList = parameters.get("bandList");
//        if (bandList != null) {
//            List<String> bandNameList = new ArrayList<>();
//            bandNameList.addAll(Arrays.asList(bandList.split(",")));
//            for (int i = 0; i < bandListBox.getItemCount(); i++) {
//                String bandname = bandListBox.getItemText(i);
//                boolean isSelected = bandNameList.contains(bandname);
//                if (isSelected) {
//                    bandListBox.setItemSelected(i, true);
//                    bandNameList.remove(bandname);
//                } else {
//                    bandListBox.setItemSelected(i, false);
//                }
//            }
//            if (!bandNameList.isEmpty()) {
//                // not all given bandnames are in the listbox
//                for (int i = 0; i < bandListBox.getItemCount(); i++) {
//                    bandListBox.setItemSelected(i, false);
//                }
//                bandNames.setText(bandList);
//            } else {
//                bandNames.setText("");
//            }
//        }
    }
    
    // fires chage event after each key press
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
