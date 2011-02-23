package com.bc.calvalus.portal.client;

import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
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
    private DateBox fromDate;
    private DateBox toDate;
    private IntegerBox periodLength;
    private DoubleBox fromLon;
    private DoubleBox toLon;
    private DoubleBox fromLat;
    private DoubleBox toLat;
    private DoubleBox resolution;
    private CheckBox multiPeriodOn;
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
        contentParams.setWidget(0, 0, new HTML("<b>L3 Content Parameters</b>"));
        contentParams.setWidget(1, 0, new Label("Input variable(s):"));
        contentParams.setWidget(1, 1, variables);
        contentParams.setWidget(2, 0, new Label("Valid mask:"));
        contentParams.setWidget(2, 1, validMask);
        contentParams.setWidget(3, 0, new Label("Aggregation:"));
        contentParams.setWidget(3, 1, this.aggregator);
        contentParams.setWidget(4, 0, new Label("Weight coeff.:"));
        contentParams.setWidget(4, 1, this.weightCoeff);
        contentParams.setWidget(5, 0, new Label("Super-sampling:"));
        contentParams.setWidget(5, 1, this.superSampling);

        fromDate = new DateBox();
        fromDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        fromDate.setValue(DATE_FORMAT.parse("2008-06-01"));

        toDate = new DateBox();
        toDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        toDate.setValue(DATE_FORMAT.parse("2008-06-07"));
        toDate.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                updateTimeParams(true);
            }
        });

        multiPeriodOn = new CheckBox("Multi-period (time-series)");
        multiPeriodOn.setValue(false);
        multiPeriodOn.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                updateTimeParams(false);
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

        FlexTable temporalParams = new FlexTable();
        temporalParams.setWidth("100%");
        temporalParams.setCellSpacing(2);
        temporalParams.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        temporalParams.getFlexCellFormatter().setColSpan(0, 0, 3);
        temporalParams.getFlexCellFormatter().setColSpan(3, 0, 3);
        temporalParams.setWidget(0, 0, new HTML("<b>L3 Temporal Parameters</b>"));
        temporalParams.setWidget(1, 0, new Label("From date:"));
        temporalParams.setWidget(1, 1, fromDate);
        temporalParams.setWidget(2, 0, new Label("To date:"));
        temporalParams.setWidget(2, 1, toDate);
        temporalParams.setWidget(3, 0, multiPeriodOn);
        temporalParams.setWidget(4, 0, new Label("Period count:"));
        temporalParams.setWidget(4, 1, periodCount);
        temporalParams.setWidget(4, 2, new Label("periods"));
        temporalParams.setWidget(5, 0, new Label("Period length:"));
        temporalParams.setWidget(5, 1, periodLength);
        temporalParams.setWidget(5, 2, new Label("days"));

        fromLon = new DoubleBox();  // todo - validate against -180 <= x <= 180
        fromLon.setValue(-180.0);
        toLon = new DoubleBox();   // todo - validate against -180 <= x <= 180
        toLon.setValue(180.0);

        fromLat = new DoubleBox();  // todo - validate against -90 <= x <= 90
        fromLat.setValue(90.0);
        toLat = new DoubleBox();  // todo - validate against -90 <= x <= 90
        toLat.setValue(90.0);

        resolution = new DoubleBox();   // todo - validate against 0 < x <= 10
        resolution.setValue(0.0416667);

        FlexTable spatialParams = new FlexTable();
        spatialParams.setWidth("100%");
        spatialParams.setCellSpacing(2);
        spatialParams.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        spatialParams.getFlexCellFormatter().setColSpan(0, 0, 3);
        spatialParams.setWidget(0, 0, new HTML("<b>L3 Spatial Parameters</b>"));
        spatialParams.setWidget(1, 0, new Label("From longitude:"));
        spatialParams.setWidget(1, 1, fromLon);
        spatialParams.setWidget(1, 2, new Label("degree"));
        spatialParams.setWidget(2, 0, new Label("To longitude:"));
        spatialParams.setWidget(2, 1, toLon);
        spatialParams.setWidget(2, 2, new Label("degree"));
        spatialParams.setWidget(3, 0, new Label("From latitude:"));
        spatialParams.setWidget(3, 1, fromLat);
        spatialParams.setWidget(3, 2, new Label("degree"));
        spatialParams.setWidget(4, 0, new Label("To latitude:"));
        spatialParams.setWidget(4, 1, toLat);
        spatialParams.setWidget(4, 2, new Label("degree"));
        spatialParams.setWidget(5, 0, new Label("Resolution:"));
        spatialParams.setWidget(5, 1, resolution);
        spatialParams.setWidget(5, 2, new Label("degree/pixel"));

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
        if (adjustingEndTime) {
            return;
        }
        long millisPerDay = 24L * 60L * 60L * 1000L;
        long deltaMillis = toDate.getValue().getTime() - fromDate.getValue().getTime();
        int deltaDays = (int) ((millisPerDay + deltaMillis - 1) / millisPerDay);
        ;

        if (endTimeAdjusted) {
            if (multiPeriodOn.getValue()) {
                // fix 'fromDate', 'toDate' and 'periodLength'
                periodCount.setValue(1 + (deltaDays - 1) / periodLength.getValue());
            } else {
                // fix 'fromDate', 'toDate'. 'periodCount' is one.
                periodCount.setValue(1);
                periodLength.setValue(deltaDays);
            }
        } else {
            try {
                adjustingEndTime = true;
                if (multiPeriodOn.getValue()) {
                    // fix 'fromDate' and 'periodLength'
                    periodCount.setValue(deltaDays / periodLength.getValue());
                    toDate.setValue(new Date(fromDate.getValue().getTime() + periodCount.getValue() * periodLength.getValue() * millisPerDay));
                } else {
                    // fix 'fromDate' and 'periodLength'. 'periodCount' is one.
                    periodCount.setValue(1);
                    toDate.setValue(new Date(fromDate.getValue().getTime() + periodLength.getValue() * millisPerDay));
                }
            } finally {
                adjustingEndTime = false;
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
        sb.append(createParameterElement("superSampling", superSampling.getText()));
        sb.append(createParameterElement("fromDate", fromDate.getFormat().format(fromDate, fromDate.getValue())));
        sb.append(createParameterElement("toDate", toDate.getFormat().format(toDate, toDate.getValue())));
        sb.append(createParameterElement("periodLength", periodLength.getText()));
        sb.append(createParameterElement("periodCount", periodCount.getText()));
        sb.append(createParameterElement("fromLon", fromLon.getText()));
        sb.append(createParameterElement("toLon", toLon.getText()));
        sb.append(createParameterElement("fromLat", fromLat.getText()));
        sb.append(createParameterElement("toLat", toLat.getText()));
        sb.append(createParameterElement("resolution", resolution.getText()));
        sb.append("</parameters>");
        return sb.toString();
    }

    private String createParameterElement(String s1, String s2) {
        return "<" + s1 + ">" + s2 + "</" + s1 + ">";
    }

}