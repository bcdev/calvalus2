/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMapWidget;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.maps.client.geom.LatLngBounds;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiFactory;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.uibinder.client.UiHandler;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.user.datepicker.client.DateBox;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.SelectionChangeEvent;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class ProductFilterView extends Composite {

    interface ProductFilterUiBinder extends UiBinder<Widget, ProductFilterView> {
    }

    private static ProductFilterUiBinder uiBinder = GWT.create(ProductFilterUiBinder.class);

    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    @UiField
    RadioButton dateSelDateList;
    @UiField
    RadioButton dateSelDateRange;

    @UiField
    DateBox minDate;
    @UiField
    DateBox maxDate;
    @UiField
    TextBox numDays;

    @UiField
    TextArea dateList;

    @UiField
    RegionMapWidget predefinedRegions;

    private final ChangeHandler changeHandler;
    private final ListDataProvider<Region> regions;

    public ProductFilterView(ListDataProvider<Region> regions, ChangeHandler changeHandler) {
        this.regions = regions;
        this.changeHandler = changeHandler;

        initWidget(uiBinder.createAndBindUi(this));


        dateSelDateRange.setValue(true);
        dateSelDateRange.addValueChangeHandler(new TimeSelValueChangeHandler());

        minDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        minDate.setValue(DATE_FORMAT.parse("2008-06-01"));
        minDate.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                ProductFilterView.this.changeHandler.dateChanged(getValueMap());
            }
        });

        maxDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        maxDate.setValue(DATE_FORMAT.parse("2008-06-10"));
        maxDate.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                ProductFilterView.this.changeHandler.dateChanged(getValueMap());
            }
        });

        dateSelDateList.setValue(false);
        dateSelDateList.addValueChangeHandler(new TimeSelValueChangeHandler());

        dateList.setEnabled(false);

        predefinedRegions.setSize("100%", "240px");
        predefinedRegions.getRegionSelectionModel().addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent selectionChangeEvent) {
                ProductFilterView.this.changeHandler.regionChanged(getValueMap());
            }
        });

    }

    @UiFactory
    RegionMapWidget makeRegionMapWidget() { // method name is insignificant
      return RegionMapWidget.create(regions, false);
    }


    // TODO this seems to be wrong ?!? (mz)
//    @UiHandler("minDate")
//    void valueChange(ValueChangeEvent<Date> event) {
//        changeHandler.dateChanged(getValueMap());
//    }

    public void validateForm() throws ValidationException {
    }

    public Map<String, String> getValueMap() {

        Map<String, String> parameters = new HashMap<String, String>();

        if (dateSelDateRange.getValue()) {
            parameters.put("minDate", minDate.getFormat().format(minDate, minDate.getValue()));
            parameters.put("maxDate", maxDate.getFormat().format(maxDate, maxDate.getValue()));
        } else {
            parameters.put("dateList", dateList.getValue());
        }

        Region region = predefinedRegions.getRegionSelectionModel().getSelectedRegion();
        if (region != null) {
            Polygon polygon = region.getPolygon();
            LatLngBounds bounds = polygon.getBounds();
            parameters.put("regionName", region.getName());
            parameters.put("regionWKT", region.getWkt());
            parameters.put("minLon", bounds.getNorthEast().getLongitude() + "");
            parameters.put("minLat", bounds.getNorthEast().getLatitude() + "");
            parameters.put("maxLon", bounds.getSouthWest().getLongitude() + "");
            parameters.put("maxLat", bounds.getSouthWest().getLatitude() + "");
        }

        return parameters;
    }

    public interface ChangeHandler {
        void dateChanged(Map<String, String> data);

        void regionChanged(Map<String, String> data);
    }

    private class TimeSelValueChangeHandler implements ValueChangeHandler<Boolean> {
        @Override
        public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
            minDate.setEnabled(dateSelDateRange.getValue());
            maxDate.setEnabled(dateSelDateRange.getValue());
            dateList.setEnabled(dateSelDateList.getValue());
            ProductFilterView.this.changeHandler.dateChanged(getValueMap());
        }
    }
}