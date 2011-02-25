package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalParameter;
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

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L3ProcessorPanel implements IsWidget {
    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private DecoratorPanel widget;

    private ListBox inputVariables;
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

        inputVariables = new ListBox();
        inputVariables.addItem("chl_conc");
        inputVariables.addItem("tsm");
        inputVariables.addItem("Z90_max");
        inputVariables.addItem("chiSquare");
        inputVariables.addItem("turbidity_index");
        inputVariables.setSelectedIndex(0);

        validMask = new TextBox();
        validMask.setText("!l1_flags.INVALID AND !l1_flags.LAND_OCEAN AND !l1p_flags.F_CLOUD");

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
        contentParams.setWidget(1, 0, new Label("L2 variable:"));
        contentParams.setWidget(1, 1, inputVariables);
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
        timeRange.add(new Label(" to "));
        timeRange.add(dateStop);

        HorizontalPanel period = new HorizontalPanel();
        period.add(periodCount);
        period.add(new Label(" x "));
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
        lonRange.add(new Label(" to "));
        lonRange.add(lonMax);

        latMin = new DoubleBox();  // todo - validate against -90 <= x <= 90
        latMin.setValue(-90.0);
        latMax = new DoubleBox();  // todo - validate against -90 <= x <= 90
        latMax.setValue(90.0);

        HorizontalPanel latRange = new HorizontalPanel();
        latRange.add(latMin);
        latRange.add(new Label(" to "));
        latRange.add(latMax);

        resolution = new DoubleBox();   // todo - validate against 0 < x <= 100
        resolution.setValue(9.28);

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
        spatialParams.setWidget(3, 2, new Label("km/pixel"));

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

    public String validate() {
        boolean weightCoeffValid = weightCoeff.getValue() >= 0.0 && weightCoeff.getValue() <= 1.0;
        if (!weightCoeffValid) {
            return "Weight coefficient must be >= 0 and <= 1.";
        }

        boolean periodCountValid = periodCount.getValue() >= 1;
        if (!periodCountValid) {
            return "Period count must be >= 1.";
        }

        boolean periodLengthValid = periodLength.getValue() >= 1;
        if (!periodLengthValid) {
            return "Period length must be >= 1.";
        }

        boolean lonMinValid = lonMin.getValue() >= -180 && lonMin.getValue() < +180;
        if (!lonMinValid) {
            return "Minimum longitude must be >= -180 and < +180 degree.";
        }
        boolean lonMaxValid = lonMax.getValue() > -180 && lonMax.getValue() <= +180;
        if (!lonMaxValid) {
            return "Maximum longitude must be > -180 and <= +180 degree.";
        }
        boolean lonMinMaxValid = lonMax.getValue() > lonMin.getValue();
        if (!lonMinMaxValid) {
            return "Maximum longitude must greater than minimum longitude.";
        }

        boolean latMinValid = latMin.getValue() >= -90 && latMin.getValue() < +90;
        if (!latMinValid) {
            return "Minimum latitude must be >= -90 and < +90 degree.";
        }
        boolean latMaxValid = latMax.getValue() > -90 && latMax.getValue() <= +90;
        if (!latMaxValid) {
            return "Maximum latitude must be > -90 and <= +90 degree.";
        }
        boolean latMinMaxValid = latMax.getValue() > latMin.getValue();
        if (!latMinMaxValid) {
            return "Maximum latitude must greater than minimum latitude.";
        }

        boolean resolutionValid = resolution.getValue() > 0.0;
        if (!resolutionValid) {
            return "Resolution must greater than zero.";
        }

        boolean superSamplingValid = superSampling.getValue() >= 1 && superSampling.getValue() <= 9;
        if (!superSamplingValid) {
            return "Super-sampling must be >= 1 and <= 9.";
        }

        return null;
    }

    public List<PortalParameter> getParameterList() {
        return Arrays.asList(
                new PortalParameter("inputVariables", inputVariables.getValue(inputVariables.getSelectedIndex())),
                new PortalParameter("validMask", validMask.getText()),
                new PortalParameter("aggregator", aggregator.getValue(aggregator.getSelectedIndex())),
                new PortalParameter("weightCoeff", weightCoeff.getText()),
                new PortalParameter("dateStart", dateStart.getFormat().format(dateStart, dateStart.getValue())),
                new PortalParameter("dateStop", dateStop.getFormat().format(dateStop, dateStop.getValue())),
                new PortalParameter("periodCount", periodCount.getText()),
                new PortalParameter("periodLength", periodLength.getText()),
                new PortalParameter("lonMin", lonMin.getText()),
                new PortalParameter("lonMax", lonMax.getText()),
                new PortalParameter("latMin", latMin.getText()),
                new PortalParameter("latMax", latMax.getText()),
                new PortalParameter("resolution", resolution.getText()),
                new PortalParameter("superSampling", superSampling.getText()));
    }
}