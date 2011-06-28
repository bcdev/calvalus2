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
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.maps.client.geom.LatLngBounds;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiFactory;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.*;
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
public class ProductSetFilterForm extends Composite {

    interface ProductFilterUiBinder extends UiBinder<Widget, ProductSetFilterForm> {
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
    RegionMapWidget regionMap;
    @UiField
    Anchor manageRegionsAnchor;

    private final ListDataProvider<Region> regions;

    public ProductSetFilterForm(final PortalContext portal) {
        this.regions = portal.getRegions();

        initWidget(uiBinder.createAndBindUi(this));

        minDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        minDate.setValue(DATE_FORMAT.parse("2008-06-01"));

        maxDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        maxDate.setValue(DATE_FORMAT.parse("2008-06-10"));

        dateSelDateRange.setValue(true);

        dateSelDateRange.addValueChangeHandler(new TimeSelValueChangeHandler());
        dateSelDateList.addValueChangeHandler(new TimeSelValueChangeHandler());

        dateList.setEnabled(false);

        manageRegionsAnchor.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                portal.showView(ManageRegionsView.ID);
            }
        });
    }

    public void addChangeHandler(final ChangeHandler changeHandler) {
        ValueChangeHandler<Date> dateValueChangeHandler = new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                changeHandler.dateChanged(getValueMap());
            }
        };
        minDate.addValueChangeHandler(dateValueChangeHandler);
        maxDate.addValueChangeHandler(dateValueChangeHandler);
        ValueChangeHandler<Boolean> booleanValueChangeHandler = new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
                changeHandler.dateChanged(getValueMap());
            }
        };
        dateSelDateRange.addValueChangeHandler(booleanValueChangeHandler);
        dateSelDateList.addValueChangeHandler(booleanValueChangeHandler);
        dateList.addValueChangeHandler(new ValueChangeHandler<String>() {
            @Override
            public void onValueChange(ValueChangeEvent<String> stringValueChangeEvent) {
                 changeHandler.dateChanged(getValueMap());
            }
        });
        regionMap.getRegionSelectionModel().addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent selectionChangeEvent) {
                changeHandler.regionChanged(getValueMap());
            }
        });
    }

    @UiFactory
    RegionMapWidget createRegionMapWidget() { // method name is insignificant
        return RegionMapWidget.create(regions, false);
    }

    public HasValue<Date> getMinDate() {
        return minDate;
    }

    public HasValue<Date> getMaxDate() {
        return maxDate;
    }

    public Region[] getSelectedRegions() {
        return regionMap.getRegionSelectionModel().getSelectedRegions();
    }

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

        Region region = regionMap.getRegionSelectionModel().getSelectedRegion();
        if (region != null) {
            Polygon polygon = region.createPolygon();
            LatLngBounds bounds = polygon.getBounds();
            parameters.put("regionName", region.getName());
            parameters.put("regionWKT", region.getGeometryWkt());
            parameters.put("minLon", bounds.getNorthEast().getLongitude() + "");
            parameters.put("minLat", bounds.getNorthEast().getLatitude() + "");
            parameters.put("maxLon", bounds.getSouthWest().getLongitude() + "");
            parameters.put("maxLat", bounds.getSouthWest().getLatitude() + "");
        } else {
            parameters.put("regionName", "World");
            parameters.put("regionWKT", "POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))");
            parameters.put("minLon", "-180");
            parameters.put("minLat", "-90");
            parameters.put("maxLon", "180");
            parameters.put("maxLat", "90");
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
        }
    }
}