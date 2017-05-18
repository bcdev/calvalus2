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
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
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

    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private final PortalContext portalContext;
    private final RABandTable bandTable;
    

    interface TheUiBinder extends UiBinder<Widget, RAConfigForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    CalvalusStyle style;
    
    @UiField
    TextBox maskExpr;
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

    public RAConfigForm(final PortalContext portalContext) {
        this.portalContext = portalContext;
        this.inputVarNames = new ArrayList<String>();

        bandTable = new RABandTable();
        bandCellTable = bandTable.getCellTable();
        
        initWidget(uiBinder.createAndBindUi(this));

        ValueChangeHandler<String> periodCountUpdater = event -> updatePeriodCount();

        steppingPeriodLength.setValue("10");
        steppingPeriodLength.addValueChangeHandler(periodCountUpdater);

        compositingPeriodLength.setValue("10");
        compositingPeriodLength.addValueChangeHandler(periodCountUpdater);

        periodsCount.setText("");
        periodsCount.setEnabled(false);
        periodsListBox.setEnabled(false);

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
}
