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

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Form for RA (region analysis) parameters.
 *
 * @author MarcoZ
 */
public class RAConfigForm extends Composite {

    private final PortalContext portalContext;

    interface TheUiBinder extends UiBinder<Widget, RAConfigForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox bandListBox;
    @UiField
    TextBox maskExpr;
    @UiField
    IntegerBox steppingPeriodLength;
    @UiField
    IntegerBox compositingPeriodLength;
    @UiField
    IntegerBox periodCount;

    private Date minDate;
    private Date maxDate;

    public RAConfigForm(final PortalContext portalContext) {
        this.portalContext = portalContext;

        initWidget(uiBinder.createAndBindUi(this));

        ValueChangeHandler<Integer> periodCountUpdater = new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updatePeriodCount();
            }
        };

        steppingPeriodLength.setValue(10);
        steppingPeriodLength.addValueChangeHandler(periodCountUpdater);

        compositingPeriodLength.setValue(10);
        compositingPeriodLength.addValueChangeHandler(periodCountUpdater);

        periodCount.setValue(0);
        periodCount.setEnabled(false);
    }

    public void updateTemporalParameters(Date minDate, Date maxDate) {
        this.minDate = minDate;
        this.maxDate = maxDate;
        updatePeriodCount();
    }

    private void updatePeriodCount() {
        if (minDate != null && maxDate != null) {
            periodCount.setValue(L3ConfigUtils.getPeriodCount(minDate,
                                                maxDate,
                                                steppingPeriodLength.getValue(),
                                                compositingPeriodLength.getValue()));
        } else {
            periodCount.setValue(0);
        }
    }

    public void validateForm() throws ValidationException {
        boolean periodCountValid = periodCount.getValue() != null && periodCount.getValue() >= 1;
        if (!periodCountValid) {
            throw new ValidationException(periodCount, "Period count must be >= 1");
        }

        Integer steppingP = steppingPeriodLength.getValue();
        boolean periodLengthValid = steppingP != null && (steppingP >= 1 || steppingP == -7 || steppingP == -30);
        if (!periodLengthValid) {
            throw new ValidationException(steppingPeriodLength, "Period length must be >= 1");
        }

        Integer compositingP = compositingPeriodLength.getValue();
        boolean compositingPeriodLengthValid = compositingP != null &&
                (compositingP >= 1 || compositingP == -7 || compositingP == -30);
        if (!compositingPeriodLengthValid) {
            throw new ValidationException(compositingPeriodLength,
                                          "Compositing period length must be >= 1");
        }
        if (bandListBox.getSelectedIndex() == -1) {
            throw new ValidationException(bandListBox, "One or more bands must be selected.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("maskExpr", maskExpr.getText());
        parameters.put("periodLength", steppingPeriodLength.getValue().toString());
        parameters.put("compositingPeriodLength", compositingPeriodLength.getValue().toString());
        parameters.put("bandList", createBandListString());
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
        maskExpr.setValue(parameters.getOrDefault("maskExpr", ""));
        String periodLengthValue = parameters.get("periodLength");
        if (periodLengthValue != null) {
            steppingPeriodLength.setValue(Integer.valueOf(periodLengthValue), true);
        }
        String compositingPeriodLengthValue = parameters.get("compositingPeriodLength");
        if (compositingPeriodLengthValue != null) {
            compositingPeriodLength.setValue(Integer.valueOf(compositingPeriodLengthValue), true);
        }
        String bandList = parameters.get("bandList");
        bandListBox.clear();
        if (bandList != null) {
            int index = 0;
            for (String bandname : bandList.split(",")) {
                bandListBox.addItem(bandname);
                bandListBox.setItemSelected(index, true);
                index++;
            }
        }
    }

}
