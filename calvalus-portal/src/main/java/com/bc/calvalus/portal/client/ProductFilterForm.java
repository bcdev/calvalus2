package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMapWidget;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.maps.client.geom.LatLngBounds;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.Label;
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
 * @deprecated use ProductFilterView
 */
@Deprecated
public class ProductFilterForm implements IsWidget {
    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private Widget widget;

    private RadioButton dateSelDateList;
    private RadioButton dateSelDateRange;

    private DateBox minDate;
    private DateBox maxDate;
    private TextBox numDays;

    private TextArea dateList;

    private RegionMapWidget predefinedRegions;

    private final ChangeHandler changeHandler;


    public ProductFilterForm(ListDataProvider<Region> regions, ChangeHandler changeHandler) {
        this.changeHandler = changeHandler;

        dateSelDateRange = new RadioButton("dateSel", "Date range");
        dateSelDateRange.setValue(true);
        dateSelDateRange.addValueChangeHandler(new TimeSelValueChangeHandler());

        minDate = new DateBox();
        minDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        minDate.setValue(DATE_FORMAT.parse("2008-06-01"));
        minDate.setWidth("6em");
        minDate.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                ProductFilterForm.this.changeHandler.dateChanged(getValueMap());
            }
        });

        maxDate = new DateBox();
        maxDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        maxDate.setValue(DATE_FORMAT.parse("2008-06-10"));
        maxDate.setWidth("6em");
        maxDate.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                ProductFilterForm.this.changeHandler.dateChanged(getValueMap());
            }
        });

        numDays = new TextBox();
        numDays.setVisibleLength(4);
        numDays.setEnabled(false);

        dateSelDateList = new RadioButton("dateSel", "Date list");
        dateSelDateList.setValue(false);
        dateSelDateList.addValueChangeHandler(new TimeSelValueChangeHandler());

        dateList = new TextArea();
        dateList.setWidth("100%");
        dateList.setVisibleLines(4);
        dateList.setEnabled(false);

        predefinedRegions = RegionMapWidget.create(regions, false);
        predefinedRegions.setSize("100%", "240px");
        predefinedRegions.getRegionSelectionModel().addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent selectionChangeEvent) {
                ProductFilterForm.this.changeHandler.regionChanged(getValueMap());
            }
        });

        widget = createContentPanel();
        widget.setWidth("100%");
    }

    private FlexTable createContentPanel() {

        FlexTable table = new FlexTable();
        FlexTable.FlexCellFormatter formatter = table.getFlexCellFormatter();

        table.setWidth("100%");
        table.setCellSpacing(2);

        int row = 0;

        table.setWidget(row, 0, new HTML("<b>Time Filter</b>"));
        formatter.setHorizontalAlignment(row, 0, HasHorizontalAlignment.ALIGN_CENTER);
        formatter.setColSpan(row, 0, 2);

        row++;
        table.setWidget(row, 0, createDateTable());

        row++;
        table.setWidget(row, 0, new HTML("<b>Region Filter</b>"));
        formatter.setHorizontalAlignment(row, 0, HasHorizontalAlignment.ALIGN_CENTER);

        row++;
        table.setWidget(row, 0, predefinedRegions);
        formatter.setColSpan(row, 0, 2);
        return table;
    }

    private FlexTable createDateTable() {
        FlexTable table = new FlexTable();
        FlexTable.FlexCellFormatter formatter = table.getFlexCellFormatter();

        table.setWidth("100%");
        table.setCellSpacing(2);

        int row = 0;

        table.setWidget(row, 0, dateSelDateRange);
        formatter.setColSpan(row, 0, 3);
        table.setWidget(row, 1, dateSelDateList);
        formatter.setColSpan(row, 1, 2);

        row++;
        table.setWidget(row, 0, new HTML("&nbsp;&nbsp;"));
        table.setWidget(row, 1, new Label("Start time:"));
        table.setWidget(row, 2, minDate);
        table.setWidget(row, 3, new HTML("&nbsp;&nbsp;"));
        formatter.setRowSpan(row, 3, 3);
        table.setWidget(row, 4, dateList);
        formatter.setRowSpan(row, 4, 3);

        row++;
        table.setWidget(row, 0, new HTML("&nbsp;&nbsp;"));
        table.setWidget(row, 1, new Label("End time:"));
        table.setWidget(row, 2, maxDate);

        row++;
        table.setWidget(row, 0, new HTML("&nbsp;&nbsp;"));
        table.setWidget(row, 1, new Label("Day count:"));
        table.setWidget(row, 2, numDays);

        return table;
    }

    @Override
    public Widget asWidget() {
        return widget;
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
            ProductFilterForm.this.changeHandler.dateChanged(getValueMap());
        }
    }
}