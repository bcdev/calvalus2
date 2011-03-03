package com.bc.calvalus.portal.client;

import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.user.datepicker.client.DateBox;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L2ProductFilterPanel implements IsWidget {
    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private DecoratorPanel widget;

    private DateBox dateStart;
    private DateBox dateStop;

    private DoubleBox lonMin;
    private DoubleBox lonMax;
    private DoubleBox latMin;
    private DoubleBox latMax;

    public L2ProductFilterPanel() {

        dateStart = new DateBox();
        dateStart.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        dateStart.setValue(DATE_FORMAT.parse("2008-06-01"));
        dateStart.setWidth("6em");

        dateStop = new DateBox();
        dateStop.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        dateStop.setValue(DATE_FORMAT.parse("2008-06-07"));
        dateStop.setWidth("6em");

        HorizontalPanel timeRange = new HorizontalPanel();
        timeRange.add(dateStart);
        timeRange.add(new HTML("&nbsp;to&nbsp;"));
        timeRange.add(dateStop);

        FlexTable temporalParams = new FlexTable();
        temporalParams.setWidth("100%");
        temporalParams.setCellSpacing(2);
        temporalParams.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        temporalParams.getFlexCellFormatter().setColSpan(0, 0, 3);
        temporalParams.getFlexCellFormatter().setColSpan(1, 1, 2);
        temporalParams.setWidget(0, 0, new HTML("<b>L3 Temporal Parameters</b>"));
        temporalParams.setWidget(1, 0, new Label("Time range:"));
        temporalParams.setWidget(1, 1, timeRange);

        lonMin = new DoubleBox();  // todo - validate against -180 <= x <= 180
        lonMin.setValue(3.0);
        lonMin.setWidth("6em");
        lonMax = new DoubleBox();   // todo - validate against -180 <= x <= 180
        lonMax.setValue(14.5);
        lonMax.setWidth("6em");

        HorizontalPanel lonRange = new HorizontalPanel();
        lonRange.add(lonMin);
        lonRange.add(new HTML("&nbsp;to&nbsp;"));
        lonRange.add(lonMax);

        latMin = new DoubleBox();  // todo - validate against -90 <= x <= 90
        latMin.setValue(52.0);
        latMin.setWidth("6em");
        latMax = new DoubleBox();  // todo - validate against -90 <= x <= 90
        latMax.setValue(56.5);
        latMax.setWidth("6em");

        HorizontalPanel latRange = new HorizontalPanel();
        latRange.add(latMin);
        latRange.add(new HTML("&nbsp;to&nbsp;"));
        latRange.add(latMax);

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

    @Override
    public Widget asWidget() {
        return widget;
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("dateStart", dateStart.getFormat().format(dateStart, dateStart.getValue()));
        parameters.put("dateStop", dateStop.getFormat().format(dateStop, dateStop.getValue()));
        parameters.put("lonMin", lonMin.getText());
        parameters.put("lonMax", lonMax.getText());
        parameters.put("latMin", latMin.getText());
        parameters.put("latMax", latMax.getText());
        return parameters;
    }
}