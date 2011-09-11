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
import com.bc.calvalus.portal.client.map.RegionMap;
import com.bc.calvalus.portal.client.map.RegionMapWidget;
import com.bc.calvalus.portal.shared.DtoProductSet;
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
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.user.datepicker.client.DateBox;
import com.google.gwt.view.client.SelectionChangeEvent;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class ProductSetFilterForm extends Composite {

    private final PortalContext portal;

    interface TheUiBinder extends UiBinder<Widget, ProductSetFilterForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    @UiField
    RadioButton temporalFilterOff;
    @UiField
    RadioButton temporalFilterByDateList;
    @UiField
    RadioButton temporalFilterByDateRange;

    @UiField
    DateBox minDateProductSet;
    @UiField
    DateBox maxDateProductSet;

    @UiField
    DateBox minDate;
    @UiField
    DateBox maxDate;
    @UiField
    TextBox numDays;

    @UiField
    TextArea dateList;

    @UiField
    RadioButton spatialFilterOff;
    @UiField
    RadioButton spatialFilterByRegion;
    @UiField
    Anchor manageRegionsAnchor;
    @UiField
    RegionMapWidget regionMap;

    static int radioGroupId;

    private DtoProductSet productSet;

    public ProductSetFilterForm(final PortalContext portal) {
        this.portal = portal;
        initWidget(uiBinder.createAndBindUi(this));

        radioGroupId++;

        minDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        minDate.setValue(DATE_FORMAT.parse("2008-05-01"));

        maxDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        maxDate.setValue(DATE_FORMAT.parse("2008-07-31"));

        minDateProductSet.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        maxDateProductSet.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));

        dateList.setEnabled(false);
        dateList.setValue("2008-06-01\n" +
                                  "2008-06-02\n" +
                                  "2008-06-03");

        temporalFilterOff.setName("temporalFilter" + radioGroupId);
        temporalFilterByDateRange.setName("temporalFilter" + radioGroupId);
        temporalFilterByDateList.setName("temporalFilter" + radioGroupId);
        temporalFilterByDateRange.setValue(true);

        ValueChangeHandler<Boolean> valueChangeHandler = new TimeSelValueChangeHandler();
        temporalFilterOff.addValueChangeHandler(valueChangeHandler);
        temporalFilterByDateRange.addValueChangeHandler(valueChangeHandler);
        temporalFilterByDateList.addValueChangeHandler(valueChangeHandler);

        spatialFilterOff.setName("spatialFilter" + radioGroupId);
        spatialFilterByRegion.setName("spatialFilter" + radioGroupId);
        spatialFilterByRegion.setValue(true);
        //TODO - how to enable /disable
//        spatialFilterOff.addValueChangeHandler(valueChangeHandler);
//        spatialFilterByRegion.addValueChangeHandler(valueChangeHandler);

        manageRegionsAnchor.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                portal.showView(ManageRegionsView.ID);
            }
        });

        addChangeHandler(new ChangeHandler() {
            @Override
            public void temporalFilterChanged(Map<String, String> data) {
                updateNumDays();
            }

            @Override
            public void spatialFilterChanged(Map<String, String> data) {
            }
        });

        updateNumDays();
    }

    private void updateNumDays() {
        long millisPerDay = 24L * 60L * 60L * 1000L;
        if (temporalFilterByDateRange.getValue()) {
            Date min = minDate.getValue();
            Date max = maxDate.getValue();
            numDays.setValue("" + ((millisPerDay + max.getTime()) - min.getTime()) / millisPerDay);
        } else if (temporalFilterByDateList.getValue()) {
            String[] splits = dateList.getValue().split("\\s");
            HashSet<String> set = new HashSet<String>(Arrays.asList(splits));
            numDays.setValue("" + set.size());
        } else if (productSet != null) {
            Date min = productSet.getMinDate();
            Date max = productSet.getMaxDate();
            numDays.setValue("" + ((millisPerDay + max.getTime()) - min.getTime()) / millisPerDay);
        }
    }

    public void setProductSet(DtoProductSet productSet) {
        this.productSet = productSet;
        if (productSet != null) {
            minDateProductSet.setValue(productSet.getMinDate());
            maxDateProductSet.setValue(productSet.getMaxDate());
        } else {
            minDateProductSet.setValue(null);
            maxDateProductSet.setValue(null);
        }
    }


    public void addChangeHandler(final ChangeHandler changeHandler) {
        ValueChangeHandler<Date> dateValueChangeHandler = new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                changeHandler.temporalFilterChanged(getValueMap());
            }
        };
        minDate.addValueChangeHandler(dateValueChangeHandler);
        maxDate.addValueChangeHandler(dateValueChangeHandler);
        ValueChangeHandler<Boolean> booleanValueChangeHandler = new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
                changeHandler.temporalFilterChanged(getValueMap());
            }
        };
        temporalFilterOff.addValueChangeHandler(booleanValueChangeHandler);
        temporalFilterByDateRange.addValueChangeHandler(booleanValueChangeHandler);
        temporalFilterByDateList.addValueChangeHandler(booleanValueChangeHandler);
        dateList.addValueChangeHandler(new ValueChangeHandler<String>() {
            @Override
            public void onValueChange(ValueChangeEvent<String> stringValueChangeEvent) {
                changeHandler.temporalFilterChanged(getValueMap());
            }
        });

        ValueChangeHandler<Boolean> spatialFilterChangeHandler = new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
                changeHandler.spatialFilterChanged(getValueMap());
            }
        };
        spatialFilterOff.addValueChangeHandler(spatialFilterChangeHandler);
        spatialFilterByRegion.addValueChangeHandler(spatialFilterChangeHandler);
        regionMap.getRegionMapSelectionModel().addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent selectionChangeEvent) {
                changeHandler.spatialFilterChanged(getValueMap());
            }
        });
    }

    // note: factory is found solely for return type, method name is insignificant
    @UiFactory
    public RegionMapWidget createRegionMap() {
        return new RegionMapWidget(portal.getRegionMapModel(), false);
    }

    public Region getSelectedRegion() {
        return spatialFilterByRegion.getValue() ? regionMap.getRegionMapSelectionModel().getSelectedRegion() : null;
    }

    public RegionMap getRegionMap() {
        return regionMap;
    }

    public void validateForm() throws ValidationException {

        if (temporalFilterByDateRange.getValue()) {
            Date min = minDate.getValue();
            Date max = maxDate.getValue();
            if (!min.before(max)) {
                throw new ValidationException(minDate, "Start date must be before end date.");
            }
            if (productSet != null) {
                if (!min.after(productSet.getMinDate())) {
                    throw new ValidationException(minDate, "Start date must be after start date of product set.");
                }
                if (!max.before(productSet.getMaxDate())) {
                    throw new ValidationException(minDate, "Stop date must be before end date of product set.");
                }
            }
        } else if (temporalFilterByDateList.getValue()) {
            String value = dateList.getValue().trim();
            if (value.isEmpty()) {
                throw new ValidationException(dateList, "Date list must not be empty.");
            }
            //TODO validate datelist against productSet.min/max
        }

        if (spatialFilterByRegion.getValue()) {
            Region region = getSelectedRegion();
            if (region == null) {
                throw new ValidationException(regionMap, "Please select a region.");
            }
        }
    }

    public Map<String, String> getValueMap() {

        Map<String, String> parameters = new HashMap<String, String>();

        if (temporalFilterOff.getValue() && productSet != null) {
            parameters.put("minDate", DATE_FORMAT.format(productSet.getMinDate()));
            parameters.put("maxDate", DATE_FORMAT.format(productSet.getMaxDate()));
        } else if (temporalFilterByDateRange.getValue()) {
            parameters.put("minDate", minDate.getFormat().format(minDate, minDate.getValue()));
            parameters.put("maxDate", maxDate.getFormat().format(maxDate, maxDate.getValue()));
        } else if (temporalFilterByDateList.getValue()) {
            parameters.put("dateList", dateList.getValue());
        }

        if (spatialFilterOff.getValue()) {
            parameters.put("regionName", "global.World");
            parameters.put("regionWKT", "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))");
            parameters.put("minLon", "-180");
            parameters.put("minLat", "-90");
            parameters.put("maxLon", "180");
            parameters.put("maxLat", "90");
        } else if (spatialFilterByRegion.getValue()) {
            Region region = getSelectedRegion();
            if (region != null) {
                Polygon polygon = region.createPolygon();
                LatLngBounds bounds = polygon.getBounds();
                parameters.put("regionName", region.getQualifiedName());
                parameters.put("regionWKT", region.getGeometryWkt());
                parameters.put("minLon", bounds.getNorthEast().getLongitude() + "");
                parameters.put("minLat", bounds.getNorthEast().getLatitude() + "");
                parameters.put("maxLon", bounds.getSouthWest().getLongitude() + "");
                parameters.put("maxLat", bounds.getSouthWest().getLatitude() + "");
            }
        }

        return parameters;
    }

    public interface ChangeHandler {
        void temporalFilterChanged(Map<String, String> data);

        void spatialFilterChanged(Map<String, String> data);
    }

    private class TimeSelValueChangeHandler implements ValueChangeHandler<Boolean> {
        @Override
        public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
            minDate.setEnabled(temporalFilterByDateRange.getValue());
            maxDate.setEnabled(temporalFilterByDateRange.getValue());
            dateList.setEnabled(temporalFilterByDateList.getValue());

//TODO - how to enable /disable
//            regionMap.setEnabled(spatialFilterByRegion.getValue());
        }
    }
}