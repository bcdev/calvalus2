package com.bc.calvalus.portal.client;

import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasValue;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.user.datepicker.client.DateBox;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L2ProductFilterForm implements IsWidget {
    public static final int MILLIS_PER_DAY = 1000 * 60 * 60 * 24;
    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private DecoratorPanel widget;

    private DateBox minDate;
    private DateBox maxDate;

    private DoubleBox minLon;
    private DoubleBox maxLon;
    private DoubleBox minLat;
    private DoubleBox maxLat;

    public L2ProductFilterForm() {

        minDate = new DateBox();
        minDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        minDate.setValue(DATE_FORMAT.parse("2008-06-01"));
        minDate.setWidth("6em");

        maxDate = new DateBox();
        maxDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        maxDate.setValue(DATE_FORMAT.parse("2008-06-01"));
        maxDate.setWidth("6em");

        HorizontalPanel timeRange = new HorizontalPanel();
        timeRange.add(minDate);
        timeRange.add(new HTML("&nbsp;to&nbsp;"));
        timeRange.add(maxDate);

        FlexTable temporalParams = new FlexTable();
        temporalParams.setWidth("100%");
        temporalParams.setCellSpacing(2);
        temporalParams.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        temporalParams.getFlexCellFormatter().setColSpan(0, 0, 3);
        temporalParams.getFlexCellFormatter().setColSpan(1, 1, 2);
        temporalParams.setWidget(0, 0, new HTML("<b>L3 Temporal Parameters</b>"));
        temporalParams.setWidget(1, 0, new Label("Time range:"));
        temporalParams.setWidget(1, 1, timeRange);

        minLon = new DoubleBox();
        minLon.setValue(3.0);
        minLon.setWidth("6em");
        maxLon = new DoubleBox();
        maxLon.setValue(14.5);
        maxLon.setWidth("6em");

        HorizontalPanel lonRange = new HorizontalPanel();
        lonRange.add(minLon);
        lonRange.add(new HTML("&nbsp;to&nbsp;"));
        lonRange.add(maxLon);

        minLat = new DoubleBox();
        minLat.setValue(52.0);
        minLat.setWidth("6em");
        maxLat = new DoubleBox();
        maxLat.setValue(56.5);
        maxLat.setWidth("6em");

        HorizontalPanel latRange = new HorizontalPanel();
        latRange.add(minLat);
        latRange.add(new HTML("&nbsp;to&nbsp;"));
        latRange.add(maxLat);

        FlexTable filterParams = new FlexTable();
        filterParams.setWidth("100%");
        filterParams.setCellSpacing(2);
        filterParams.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        filterParams.getFlexCellFormatter().setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_CENTER);
        filterParams.getFlexCellFormatter().setColSpan(0, 0, 3);
        filterParams.getFlexCellFormatter().setColSpan(1, 1, 2);
        filterParams.getFlexCellFormatter().setColSpan(2, 0, 3);

        filterParams.setWidget(0, 0, new HTML("<b>Temporal Product Filter</b>"));

        filterParams.setWidget(1, 0, new Label("Time range:"));
        filterParams.setWidget(1, 1, timeRange);

        filterParams.setWidget(2, 0, new HTML("<b>Spatial Product Filter</b>"));

        filterParams.setWidget(3, 0, new Label("Lon. range:"));
        filterParams.setWidget(3, 1, lonRange);
        filterParams.setWidget(3, 2, new Label("deg"));

        filterParams.setWidget(4, 0, new Label("Lat. range:"));
        filterParams.setWidget(4, 1, latRange);
        filterParams.setWidget(4, 2, new Label("deg"));

        // Wrap the contents in a DecoratorPanel
        widget = new DecoratorPanel();
        widget.setTitle("L2 Product Filter");
        widget.setWidget(filterParams);
    }

    public HasValue<Date> getMinDate() {
        return minDate;
    }

    public HasValue<Date> getMaxDate() {
        return maxDate;
    }

    public void validateForm(int maxDays) throws ValidationException {
        UIUtils.validateDateBox("Min. date", this.minDate);
        UIUtils.validateDateBox("Max. date", this.maxDate);
        Date minDate = this.minDate.getValue();
        Date maxDate = this.maxDate.getValue();

        if (minDate.compareTo(maxDate) > 0) {
            throw new ValidationException(this.minDate, "Value for 'Min. date' must be less or equal to 'Max. date'");
        }
        long deltaTime = maxDate.getTime() - minDate.getTime();
        if (deltaTime / MILLIS_PER_DAY > maxDays) {
            throw new ValidationException(this.minDate, "The difference between 'Min. date' and 'Max. date' must not exceed " + maxDays + " days");
        }

        UIUtils.validateDoubleBox("Min. longitude", this.minLon, -180.0, 180.0);
        UIUtils.validateDoubleBox("Max. longitude", this.maxLon, -180.0, 180.0);
        Double minLon = this.minLon.getValue();
        Double maxLon = this.maxLon.getValue();
        if (minLon >= maxLon) {
            throw new ValidationException(this.minLon, "Value for 'Min. longitude' must be less or equal to 'Max. longitude'");
        }

        UIUtils.validateDoubleBox("Min. latitude", this.minLat, -90.0, 90.0);
        UIUtils.validateDoubleBox("Max. latitude", this.maxLat, -90.0, 90.0);
        Double minLat = this.minLat.getValue();
        Double maxLat = this.maxLat.getValue();
        if (minLat >= maxLat) {
            throw new ValidationException(this.minLat, "Value for 'Min. latitude' must be less or equal to 'Max. latitude'");
        }
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("dateStart", minDate.getFormat().format(minDate, minDate.getValue()));
        parameters.put("dateStop", maxDate.getFormat().format(maxDate, maxDate.getValue()));
        parameters.put("lonMin", minLon.getText());
        parameters.put("lonMax", maxLon.getText());
        parameters.put("latMin", minLat.getText());
        parameters.put("latMax", maxLat.getText());
        return parameters;
    }
}