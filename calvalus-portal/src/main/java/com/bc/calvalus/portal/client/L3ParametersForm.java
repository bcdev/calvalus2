package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMapWidget;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.DecoratedTabPanel;
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
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.user.datepicker.client.DateBox;
import com.google.gwt.view.client.ListDataProvider;

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
    private DateBox minDate;
    private DateBox maxDate;
    private IntegerBox steppingPeriodLength;
    private IntegerBox compositingPeriodLength;
    private IntegerBox periodCount;
    private DoubleBox minLon;
    private DoubleBox maxLon;
    private DoubleBox minLat;
    private DoubleBox maxLat;
    private DoubleBox resolution;
    private IntegerBox superSampling;
    private RegionMapWidget predefinedRegions;
    private CheckBox useBoundingBox;

    public L3ParametersForm(ListDataProvider<Region> regions) {

        inputVariables = new ListBox();
        inputVariables.addItem("chl_conc");
        inputVariables.addItem("tsm");
        inputVariables.addItem("Z90_max");
        inputVariables.addItem("chiSquare");
        inputVariables.addItem("turbidity_index");
        inputVariables.setSelectedIndex(0);

        maskExpr = new TextBox();
        maskExpr.setText("!l1_flags.INVALID AND !l1_flags.LAND_OCEAN");
        maskExpr.setWidth("24em");

        fillValue = new DoubleBox();
        fillValue.setValue(Double.NaN);

        aggregator = new ListBox();
        aggregator.addItem("Average", "AVG");
        aggregator.addItem("Max. Likelihood Average", "AVG_ML");
        aggregator.addItem("Minimum + Maximum", "MIN_MAX");
        aggregator.addItem("P90 Percentile", "PERCENTILE");
        aggregator.setVisibleItemCount(1);
        aggregator.setSelectedIndex(1);

        weightCoeff = new DoubleBox();
        weightCoeff.setValue(0.5);
        weightCoeff.setWidth("6em");

        superSampling = new IntegerBox();
        superSampling.setValue(1);
        superSampling.setWidth("2em");

        minDate = new DateBox();
        minDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        minDate.setValue(DATE_FORMAT.parse("2008-06-01"));
        minDate.setWidth("6em");
        minDate.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                updatePeriodCount();
            }
        });

        maxDate = new DateBox();
        maxDate.setFormat(new DateBox.DefaultFormat(DATE_FORMAT));
        maxDate.setValue(DATE_FORMAT.parse("2008-06-10"));
        maxDate.setWidth("6em");
        maxDate.addValueChangeHandler(new ValueChangeHandler<Date>() {
            @Override
            public void onValueChange(ValueChangeEvent<Date> event) {
                updatePeriodCount();
            }
        });

        steppingPeriodLength = new IntegerBox();
        steppingPeriodLength.setValue(10);
        steppingPeriodLength.setWidth("4em");
        steppingPeriodLength.addValueChangeHandler(new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updatePeriodCount();
            }
        });

        compositingPeriodLength = new IntegerBox();
        compositingPeriodLength.setValue(10);
        compositingPeriodLength.setWidth("4em");

        periodCount = new IntegerBox();
        periodCount.setValue(0);
        periodCount.setWidth("4em");
        periodCount.setEnabled(false);

        resolution = new DoubleBox();
        resolution.setValue(9.28);
        resolution.setWidth("6em");

        useBoundingBox = new CheckBox("Use user-defined bounding box");

        minLon = new DoubleBox();
        minLon.setValue(3.0);
        minLon.setWidth("6em");
        maxLon = new DoubleBox();
        maxLon.setValue(14.5);
        maxLon.setWidth("6em");

        minLat = new DoubleBox();
        minLat.setValue(52.0);
        minLat.setWidth("6em");
        maxLat = new DoubleBox();
        maxLat.setValue(56.5);
        maxLat.setWidth("6em");

        predefinedRegions = RegionMapWidget.create(regions, false);
        predefinedRegions.setSize("100%", "240px");

        HorizontalPanel timeRange = new HorizontalPanel();
        timeRange.add(minDate);
        timeRange.add(new HTML("&nbsp;to&nbsp;"));
        timeRange.add(maxDate);

        HorizontalPanel lonRange = new HorizontalPanel();
        lonRange.add(minLon);
        lonRange.add(new HTML("&nbsp;to&nbsp;"));
        lonRange.add(maxLon);

        HorizontalPanel latRange = new HorizontalPanel();
        latRange.add(minLat);
        latRange.add(new HTML("&nbsp;to&nbsp;"));
        latRange.add(maxLat);

        FlexTable boundingBoxPanel = new FlexTable();
        boundingBoxPanel.setWidth("100%");
        boundingBoxPanel.setCellSpacing(2);
        //boundingBoxPanel.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        boundingBoxPanel.getFlexCellFormatter().setColSpan(0, 0, 3);
        boundingBoxPanel.setWidget(0, 0, useBoundingBox);

        boundingBoxPanel.setWidget(1, 0, new Label("Lon. range:"));
        boundingBoxPanel.setWidget(1, 1, lonRange);
        boundingBoxPanel.setWidget(1, 2, new Label("deg"));

        boundingBoxPanel.setWidget(2, 0, new Label("Lat. range:"));
        boundingBoxPanel.setWidget(2, 1, latRange);
        boundingBoxPanel.setWidget(2, 2, new Label("deg"));



        DecoratedTabPanel regionTabPanel = new DecoratedTabPanel();
        regionTabPanel.ensureDebugId("regionTabPanel");
        regionTabPanel.setWidth("400px");
        regionTabPanel.setAnimationEnabled(true);
        regionTabPanel.add(predefinedRegions, "Predefined Regions");
        regionTabPanel.add(boundingBoxPanel, "Bounding Box");
        regionTabPanel.selectTab(0);

        FlexTable contentParams = new FlexTable();
        FlexTable.FlexCellFormatter cellFormatter = contentParams.getFlexCellFormatter();

        contentParams.setWidth("100%");
        contentParams.setCellSpacing(2);
        cellFormatter.setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);

        int row = 0;

        contentParams.setWidget(row, 0, new HTML("<b>L3 Parameters</b>"));
        cellFormatter.setColSpan(row, 0, 3);

        // The following must be available for multiple variables {{

        row++;
        contentParams.setWidget(row, 0, new Label("L2 variable:"));
        contentParams.setWidget(row, 1, inputVariables);
        cellFormatter.setColSpan(row, 1, 2);

        row++;
        contentParams.setWidget(row, 0, new Label("Valid mask:"));
        contentParams.setWidget(row, 1, maskExpr);
        cellFormatter.setColSpan(row, 1, 2);

        row++;
        contentParams.setWidget(row, 0, new Label("Fill value:"));
        contentParams.setWidget(row, 1, fillValue);

        row++;
        contentParams.setWidget(row, 0, new Label("Aggregation:"));
        contentParams.setWidget(row, 1, aggregator);
        cellFormatter.setColSpan(row, 1, 2);

        row++;
        contentParams.setWidget(row, 0, new Label("Weight coeff.:"));
        contentParams.setWidget(row, 1, weightCoeff);
        contentParams.setWidget(row, 2, new Label("1"));

        // }}

        row++;
        contentParams.setWidget(row, 0, new Label("Super-sampling:"));
        contentParams.setWidget(row, 1, superSampling);
        contentParams.setWidget(row, 2, new Label("pixels/pixel"));

        row++;
        contentParams.setWidget(row, 0, new HTML("<hr/>"));
        cellFormatter.setColSpan(row, 0, 3);

        row++;
        contentParams.setWidget(row, 0, new Label("Time range:"));
        contentParams.setWidget(row, 1, timeRange);
        cellFormatter.setColSpan(row, 1, 2);

        row++;
        contentParams.setWidget(row, 0, new Label("Stepping period:"));
        contentParams.setWidget(row, 1, steppingPeriodLength);
        contentParams.setWidget(row, 2, new Label("days"));

        row++;
        contentParams.setWidget(row, 0, new Label("Compositing period:"));
        contentParams.setWidget(row, 1, compositingPeriodLength);
        contentParams.setWidget(row, 2, new Label("days"));

        row++;
        contentParams.setWidget(row, 0, new Label("Number of periods:"));
        contentParams.setWidget(row, 1, periodCount);
        cellFormatter.setColSpan(row, 1, 2);

        row++;
        contentParams.setWidget(row, 0, new HTML("<hr/>"));
        cellFormatter.setColSpan(row, 0, 3);

        row++;
        contentParams.setWidget(row, 0, new Label("Resolution:"));
        contentParams.setWidget(row, 1, resolution);
        contentParams.setWidget(row, 2, new Label("km/pixel"));

        row++;
        contentParams.setWidget(row, 0, regionTabPanel);
        cellFormatter.setColSpan(row, 0, 3);

        // Wrap the contents in a DecoratorPanel
        widget = new DecoratorPanel();
        widget.setTitle("L3 Parameters");
        widget.setWidget(contentParams);

        updatePeriodCount();
    }

    private void updatePeriodCount() {
        long millisPerDay = 24L * 60L * 60L * 1000L;
        long deltaMillis = maxDate.getValue().getTime() - minDate.getValue().getTime();
        int deltaDays = (int) ((millisPerDay + deltaMillis) / millisPerDay);
        periodCount.setValue(deltaDays / steppingPeriodLength.getValue());
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    public void validateForm() throws ValidationException {
        boolean periodCountValid = periodCount.getValue() >= 1;
        if (!periodCountValid) {
            throw new ValidationException(maxDate, "Period count must be >= 1");
        }

        boolean periodLengthValid = steppingPeriodLength.getValue() >= 1;
        if (!periodLengthValid) {
            throw new ValidationException(steppingPeriodLength, "Period length must be >= 1");
        }

        boolean compositingPeriodLengthValid = compositingPeriodLength.getValue() >= 1 && compositingPeriodLength.getValue() <= steppingPeriodLength.getValue();
        if (!compositingPeriodLengthValid) {
            throw new ValidationException(compositingPeriodLength, "Compositing period length must be >= 1 and less or equal to than period");
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

        boolean weightCoeffValid = weightCoeff.getValue() >= 0.0 && weightCoeff.getValue() <= 1.0;
        if (!weightCoeffValid) {
            throw new ValidationException(weightCoeff, "Weight coefficient must be >= 0 and <= 1");
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
        Region region = predefinedRegions.getRegionSelectionModel().getSelectedRegion();

        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputVariables", inputVariables.getValue(inputVariables.getSelectedIndex()));
        parameters.put("maskExpr", maskExpr.getText());
        parameters.put("fillValue", fillValue.getText());
        parameters.put("aggregator", aggregator.getValue(aggregator.getSelectedIndex()));
        parameters.put("weightCoeff", weightCoeff.getText());
        parameters.put("minDate", minDate.getFormat().format(minDate, minDate.getValue()));
        parameters.put("maxDate", maxDate.getFormat().format(maxDate, maxDate.getValue()));
        parameters.put("periodLength", steppingPeriodLength.getText());
        parameters.put("compositingPeriodLength", compositingPeriodLength.getText());
        if (region != null) {
            parameters.put("regionName", region.getName());
            parameters.put("regionWKT", region.getWkt());
        }
        parameters.put("minLon", minLon.getText());
        parameters.put("maxLon", maxLon.getText());
        parameters.put("minLat", minLat.getText());
        parameters.put("maxLat", maxLat.getText());
        parameters.put("resolution", resolution.getText());
        parameters.put("superSampling", superSampling.getText());
        return parameters;
    }
}