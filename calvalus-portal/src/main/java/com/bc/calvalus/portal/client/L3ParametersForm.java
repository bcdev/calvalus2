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
import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L3ParametersForm implements IsWidget {
    private static final DateTimeFormat DATE_FORMAT = DateTimeFormat.getFormat("yyyy-MM-dd");

    private DecoratorPanel widget;

    private ListBox inputVariables;
    private TextBox maskExpr;
    private DoubleBox fillValue;
    private ListBox aggregator;
    private DoubleBox weightCoeff;
    private DateBox startDate;
    private DateBox stopDate;
    private IntegerBox periodLength;
    private DoubleBox minLon;
    private DoubleBox maxLon;
    private DoubleBox minLat;
    private DoubleBox maxLat;
    private DoubleBox resolution;
    private IntegerBox periodCount;
    private IntegerBox superSampling;

    boolean adjustingEndTime;

    public L3ParametersForm() {

        inputVariables = new ListBox();
        inputVariables.addItem("chl_conc");
        inputVariables.addItem("tsm");
        inputVariables.addItem("Z90_max");
        inputVariables.addItem("chiSquare");
        inputVariables.addItem("turbidity_index");
        inputVariables.setSelectedIndex(0);

        maskExpr = new TextBox();
        maskExpr.setText("!l1_flags.INVALID AND !l1_flags.LAND_OCEAN AND !l1p_flags.F_CLOUD");

        fillValue = new DoubleBox();
        fillValue.setValue(Double.NaN);

        aggregator = new ListBox();
        aggregator.addItem("Average", "AVG");
        aggregator.addItem("Max. Likelihood Average", "AVG_ML");
        aggregator.addItem("Minimum + Maximum", "MIN_MAX");
        aggregator.addItem("P90 Percentile", "PERCENTILE");
        aggregator.setVisibleItemCount(1);
        aggregator.setSelectedIndex(1);

        weightCoeff = new DoubleBox(); // todo - validate against 0 <= x <= 1.0
        weightCoeff.setValue(0.5);
        weightCoeff.setWidth("6em");

        superSampling = new IntegerBox();  // todo - validate against 1 <= x <= 33
        superSampling.setValue(1);
        superSampling.setWidth("2em");

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
        contentParams.setWidget(2, 1, maskExpr);
        contentParams.setWidget(3, 0, new Label("Fill value:"));
        contentParams.setWidget(3, 1, fillValue);
        contentParams.setWidget(4, 0, new Label("Aggregation:"));
        contentParams.setWidget(4, 1, aggregator);
        contentParams.setWidget(5, 0, new Label("Weight coeff.:"));
        contentParams.setWidget(5, 1, weightCoeff);
        contentParams.setWidget(5, 2, new Label("1"));
        contentParams.setWidget(6, 0, new Label("Super-sampling:"));
        contentParams.setWidget(6, 1, superSampling);
        contentParams.setWidget(6, 2, new Label("pixel/pixel"));

        startDate = new DateBox();
        startDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        startDate.setValue(DATE_FORMAT.parse("2008-06-01"));
        startDate.setWidth("6em");

        stopDate = new DateBox();
        stopDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        stopDate.setValue(DATE_FORMAT.parse("2008-06-07"));
        stopDate.setWidth("6em");
        stopDate.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                updateTimeParams(true);
            }
        });

        periodCount = new IntegerBox();  // todo - validate against 1 <= x <= 1000 (?)
        periodCount.setValue(1);
        periodCount.setWidth("2em");
        periodCount.addValueChangeHandler(new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updateTimeParams(false);
            }
        });

        periodLength = new IntegerBox(); // todo - validate against 1 <= x <= 365 (?)
        periodLength.setValue(7);
        periodLength.setWidth("4em");
        periodLength.addValueChangeHandler(new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updateTimeParams(false);
            }
        });

        HorizontalPanel timeRange = new HorizontalPanel();
        timeRange.add(startDate);
        timeRange.add(new HTML("&nbsp;to&nbsp;"));
        timeRange.add(stopDate);

        HorizontalPanel period = new HorizontalPanel();
        period.add(periodCount);
        period.add(new HTML("&nbsp;x&nbsp;"));
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

        minLon = new DoubleBox();  // todo - validate against -180 <= x <= 180
        minLon.setValue(3.0);
        minLon.setWidth("6em");
        maxLon = new DoubleBox();   // todo - validate against -180 <= x <= 180
        maxLon.setValue(14.5);
        maxLon.setWidth("6em");

        HorizontalPanel lonRange = new HorizontalPanel();
        lonRange.add(minLon);
        lonRange.add(new HTML("&nbsp;to&nbsp;"));
        lonRange.add(maxLon);

        minLat = new DoubleBox();  // todo - validate against -90 <= x <= 90
        minLat.setValue(52.0);
        minLat.setWidth("6em");
        maxLat = new DoubleBox();  // todo - validate against -90 <= x <= 90
        maxLat.setValue(56.5);
        maxLat.setWidth("6em");

        HorizontalPanel latRange = new HorizontalPanel();
        latRange.add(minLat);
        latRange.add(new HTML("&nbsp;to&nbsp;"));
        latRange.add(maxLat);

        resolution = new DoubleBox();   // todo - validate against 0 < x <= 100
        resolution.setValue(9.28);
        resolution.setWidth("6em");

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

    private void updateTimeParams(boolean endTimeAdjusted) {
        if (!adjustingEndTime) {
            long millisPerDay = 24L * 60L * 60L * 1000L;
            long deltaMillis = stopDate.getValue().getTime() - startDate.getValue().getTime();
            int deltaDays = (int) ((millisPerDay + deltaMillis - 1) / millisPerDay);

            if (endTimeAdjusted) {
                periodCount.setValue(1 + (deltaDays - 1) / periodLength.getValue());
            } else {
                try {
                    adjustingEndTime = true;
                    stopDate.setValue(new Date(startDate.getValue().getTime() + periodCount.getValue() * periodLength.getValue() * millisPerDay));
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

    public void validateForm() throws ValidationException {
        boolean weightCoeffValid = weightCoeff.getValue() >= 0.0 && weightCoeff.getValue() <= 1.0;
        if (!weightCoeffValid) {
            throw new ValidationException(weightCoeff, "Weight coefficient must be >= 0 and <= 1");
        }

        boolean periodCountValid = periodCount.getValue() >= 1;
        if (!periodCountValid) {
            throw new ValidationException(periodCount, "Period count must be >= 1");
        }

        boolean periodLengthValid = periodLength.getValue() >= 1;
        if (!periodLengthValid) {
            throw new ValidationException(periodLength, "Period length must be >= 1");
        }

        boolean minLonValid = minLon.getValue() >= -180 && minLon.getValue() < +180;
        if (!minLonValid) {
            throw new ValidationException(minLon, "Minimum longitude must be >= -180 and < +180 degree");
        }
        boolean maxLonValid = maxLon.getValue() > -180 && maxLon.getValue() <= +180;
        if (!maxLonValid) {
            throw new ValidationException(maxLon, "Maximum longitude must be > -180 and <= +180 degree");
        }
        boolean lonRangeValid = maxLon.getValue() > minLon.getValue();
        if (!lonRangeValid) {
            throw new ValidationException(minLon, "Maximum longitude must greater than minimum longitude");
        }

        boolean minLatValid = minLat.getValue() >= -90 && minLat.getValue() < +90;
        if (!minLatValid) {
           throw new ValidationException(weightCoeff, "Minimum latitude must be >= -90 and < +90 degree");
        }
        boolean maxLatValid = this.maxLat.getValue() > -90 && this.maxLat.getValue() <= +90;
        if (!maxLatValid) {
            throw new ValidationException(this.maxLat, "Maximum latitude must be > -90 and <= +90 degree");
        }
        boolean latRangeValid = this.maxLat.getValue() > minLat.getValue();
        if (!latRangeValid) {
            throw new ValidationException(minLat, "Maximum latitude must greater than minimum latitude.");
        }

        boolean resolutionValid = resolution.getValue() > 0.0;
        if (!resolutionValid) {
            throw new ValidationException(resolution, "Resolution must greater than zero");
        }

        boolean superSamplingValid = superSampling.getValue() >= 1 && superSampling.getValue() <= 9;
        if (!superSamplingValid) {
            throw new ValidationException(superSampling, "Super-sampling must be >= 1 and <= 9");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputVariables", inputVariables.getValue(inputVariables.getSelectedIndex()));
        parameters.put("maskExpr", maskExpr.getText());
        parameters.put("fillValue", fillValue.getText());
        parameters.put("aggregator", aggregator.getValue(aggregator.getSelectedIndex()));
        parameters.put("weightCoeff", weightCoeff.getText());
        parameters.put("dateStart", startDate.getFormat().format(startDate, startDate.getValue()));
        parameters.put("dateStop", stopDate.getFormat().format(stopDate, stopDate.getValue()));
        parameters.put("periodCount", periodCount.getText());
        parameters.put("periodLength", periodLength.getText());
        parameters.put("lonMin", minLon.getText());
        parameters.put("lonMax", maxLon.getText());
        parameters.put("latMin", minLat.getText());
        parameters.put("latMax", maxLat.getText());
        parameters.put("resolution", resolution.getText());
        parameters.put("superSampling", superSampling.getText());
        return parameters;
    }
}