package com.bc.calvalus.portal.client;

import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.user.datepicker.client.DateBox;

import java.util.Date;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L3ProcessorPanel implements IsWidget {
    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private DecoratorPanel widget;

    private TextBox variables;
    private TextBox validMask;
    private ListBox aggregator;
    private DoubleBox weightCoeff;
    private DateBox dateStart;
    private DateBox dateStop;
    private IntegerBox periodLength;
    private DoubleBox lonMin;
    private DoubleBox lonMax;
    private DoubleBox latMin;
    private DoubleBox latMax;
    private DoubleBox resolution;
    private IntegerBox periodCount;
    private IntegerBox superSampling;

    public L3ProcessorPanel() {

        variables = new TextBox();
        variables.setText("chl, tsm, gelb");

        validMask = new TextBox();
        validMask.setText("!l1_flags.INVALID && !l1p_flags.LAND && !l1p_flags.CLOUD");

        aggregator = new ListBox();
        aggregator.addItem("Average", "AVG");
        aggregator.addItem("Max. Likelihood Average", "AVG_ML");
        aggregator.addItem("Minimum + Maximum", "MIN_MAX");
        aggregator.setVisibleItemCount(1);
        aggregator.setSelectedIndex(1);

        weightCoeff = new DoubleBox(); // todo - validate against 0 <= x <= 1.0
        weightCoeff.setValue(0.5);

        superSampling = new IntegerBox();  // todo - validate against 1 <= x <= 33
        superSampling.setValue(1);

        FlexTable contentParams = new FlexTable();
        contentParams.setWidth("100%");
        contentParams.setCellSpacing(2);
        contentParams.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        contentParams.getFlexCellFormatter().setColSpan(0, 0, 3);
        contentParams.getFlexCellFormatter().setColSpan(1, 1, 2);
        contentParams.getFlexCellFormatter().setColSpan(2, 1, 2);
        contentParams.getFlexCellFormatter().setColSpan(3, 1, 2);
        contentParams.setWidget(0, 0, new HTML("<b>L3 Content Parameters</b>"));
        contentParams.setWidget(1, 0, new Label("Input variable(s):"));
        contentParams.setWidget(1, 1, variables);
        contentParams.setWidget(2, 0, new Label("Valid mask:"));
        contentParams.setWidget(2, 1, validMask);
        contentParams.setWidget(3, 0, new Label("Aggregation:"));
        contentParams.setWidget(3, 1, this.aggregator);
        contentParams.setWidget(4, 0, new Label("Weight coeff.:"));
        contentParams.setWidget(4, 1, this.weightCoeff);
        contentParams.setWidget(4, 2, new Label("1"));
        contentParams.setWidget(5, 0, new Label("Super-sampling:"));
        contentParams.setWidget(5, 1, this.superSampling);
        contentParams.setWidget(5, 2, new Label("pixel/pixel"));

        dateStart = new DateBox();
        dateStart.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        dateStart.setValue(DATE_FORMAT.parse("2008-06-01"));

        dateStop = new DateBox();
        dateStop.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        dateStop.setValue(DATE_FORMAT.parse("2008-06-07"));
        dateStop.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                updateTimeParams(true);
            }
        });

        periodCount = new IntegerBox();  // todo - validate against 1 <= x <= 1000 (?)
        periodCount.setValue(1);
        periodCount.addValueChangeHandler(new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updateTimeParams(false);
            }
        });

        periodLength = new IntegerBox(); // todo - validate against 1 <= x <= 365 (?)
        periodLength.setValue(7);
        periodLength.addValueChangeHandler(new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updateTimeParams(false);
            }
        });

        HorizontalPanel timeRange = new HorizontalPanel();
        timeRange.add(dateStart);
        timeRange.add(new Label("-"));
        timeRange.add(dateStop);

        HorizontalPanel period = new HorizontalPanel();
        period.add(periodCount);
        period.add(new Label("x"));
        period.add(periodLength);

        FlexTable temporalParams = new FlexTable();
        temporalParams.setWidth("100%");
        temporalParams.setCellSpacing(2);
        temporalParams.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        temporalParams.getFlexCellFormatter().setColSpan(0, 0, 3);
        temporalParams.getFlexCellFormatter().setColSpan(1, 1, 2);
        temporalParams.setWidget(0, 0, new HTML("<b>L3 Temporal Parameters</b>"));
        temporalParams.setWidget(1, 0, new Label("Time range:"));
        temporalParams.setWidget(1, 1, timeRange);
        temporalParams.setWidget(2, 0, new Label("Period:"));
        temporalParams.setWidget(2, 1, period);
        temporalParams.setWidget(2, 2, new Label("days"));

        lonMin = new DoubleBox();  // todo - validate against -180 <= x <= 180
        lonMin.setValue(-180.0);
        lonMax = new DoubleBox();   // todo - validate against -180 <= x <= 180
        lonMax.setValue(180.0);

        HorizontalPanel lonRange = new HorizontalPanel();
        lonRange.add(lonMin);
        lonRange.add(new Label("-"));
        lonRange.add(lonMax);

        latMin = new DoubleBox();  // todo - validate against -90 <= x <= 90
        latMin.setValue(-90.0);
        latMax = new DoubleBox();  // todo - validate against -90 <= x <= 90
        latMax.setValue(90.0);

        HorizontalPanel latRange = new HorizontalPanel();
        latRange.add(latMin);
        latRange.add(new Label("-"));
        latRange.add(latMax);

        resolution = new DoubleBox();   // todo - validate against 0 < x <= 10
        resolution.setValue(0.0416667);

        FlexTable spatialParams = new FlexTable();
        spatialParams.setWidth("100%");
        spatialParams.setCellSpacing(2);
        spatialParams.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        spatialParams.getFlexCellFormatter().setColSpan(0, 0, 3);
        spatialParams.setWidget(0, 0, new HTML("<b>L3 Spatial Parameters</b>"));
        spatialParams.setWidget(1, 0, new Label("Long. range:"));
        spatialParams.setWidget(1, 1, lonRange);
        spatialParams.setWidget(1, 2, new Label("deg"));
        spatialParams.setWidget(2, 0, new Label("Lat. range:"));
        spatialParams.setWidget(2, 1, latRange);
        spatialParams.setWidget(2, 2, new Label("deg"));
        spatialParams.setWidget(3, 0, new Label("Resolution:"));
        spatialParams.setWidget(3, 1, resolution);
        spatialParams.setWidget(3, 2, new Label("deg/pixel"));

        VerticalPanel panel = new VerticalPanel();
        panel.setSpacing(4);
        panel.add(contentParams);
        panel.add(temporalParams);
        panel.add(spatialParams);

        // Wrap the contents in a DecoratorPanel
        widget = new DecoratorPanel();
        widget.setTitle("L3 Parameters");
        widget.setWidget(panel);
    }

    boolean adjustingEndTime;

    private void updateTimeParams(boolean endTimeAdjusted) {
        if (!adjustingEndTime) {
            long millisPerDay = 24L * 60L * 60L * 1000L;
            long deltaMillis = dateStop.getValue().getTime() - dateStart.getValue().getTime();
            int deltaDays = (int) ((millisPerDay + deltaMillis - 1) / millisPerDay);

            if (endTimeAdjusted) {
                periodCount.setValue(1 + (deltaDays - 1) / periodLength.getValue());
            } else {
                try {
                    adjustingEndTime = true;
                    dateStop.setValue(new Date(dateStart.getValue().getTime() + periodCount.getValue() * periodLength.getValue() * millisPerDay));
                } finally {
                    adjustingEndTime = false;
                }
            }
        }
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    public String getProcessorParameters() {
        StringBuilder sb = new StringBuilder();
        sb.append("<parameters>");
        sb.append(createParameterElement("variables", variables.getText()));
        sb.append(createParameterElement("validMask", validMask.getText()));
        sb.append(createParameterElement("aggregator", aggregator.getValue(aggregator.getSelectedIndex())));
        sb.append(createParameterElement("weightCoeff", weightCoeff.getText()));
        sb.append(createParameterElement("dateStart", dateStart.getFormat().format(dateStart, dateStart.getValue())));
        sb.append(createParameterElement("dateStop", dateStop.getFormat().format(dateStop, dateStop.getValue())));
        sb.append(createParameterElement("periodCount", periodCount.getText()));
        sb.append(createParameterElement("periodLength", periodLength.getText()));
        sb.append(createParameterElement("lonMin", lonMin.getText()));
        sb.append(createParameterElement("lonMax", lonMax.getText()));
        sb.append(createParameterElement("latMin", latMin.getText()));
        sb.append(createParameterElement("latMax", latMax.getText()));
        sb.append(createParameterElement("resolution", resolution.getText()));
        sb.append(createParameterElement("superSampling", superSampling.getText()));
        sb.append("</parameters>");
        return sb.toString();
    }

    private String createParameterElement(String s1, String s2) {
        return "<" + s1 + ">" + s2 + "</" + s1 + ">";
    }

}