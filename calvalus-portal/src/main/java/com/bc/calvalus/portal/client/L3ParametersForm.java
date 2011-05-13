package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsRegion;
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
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.TextBox;
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
    private ListBox predefinedRegions;
    private CheckBox useBoundingBox;

    public L3ParametersForm() {
        this(getDefaultRegions());
    }

    public L3ParametersForm(GsRegion[] regions) {

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

        predefinedRegions = new ListBox(true);
        predefinedRegions.setVisibleItemCount(8);
        for (GsRegion region : regions) {
            predefinedRegions.addItem(region.getName());
        }

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
        regionTabPanel.setWidth("400px");
        regionTabPanel.setAnimationEnabled(true);
        regionTabPanel.ensureDebugId("cwRegionTabPanel");
        regionTabPanel.add(new ScrollPanel(predefinedRegions), "Predefined Regions");
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
        parameters.put("minLon", minLon.getText());
        parameters.put("maxLon", maxLon.getText());
        parameters.put("minLat", minLat.getText());
        parameters.put("maxLat", maxLat.getText());
        parameters.put("resolution", resolution.getText());
        parameters.put("superSampling", superSampling.getText());
        return parameters;
    }

    private static GsRegion[] getDefaultRegions() {
        return new GsRegion[]{
                new GsRegion("globe", "polygon((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"),
                new GsRegion("acadia", "polygon((-69.00 42.00, -52.00 42.00, -52.00 52.00, -69.00 52.00, -69.00 42.00))"),
                new GsRegion("amazondelta", "polygon((-54.00 8.00, -55.00 8.00, -55.00 5.10, -54.02 5.00, -52.04 3.96, -52.02 -0.98, -49.01 -2.99, -46.83 -1.76, -45.00 -3.50, -43.00 -3.50, -42.98 0.71, -51.06 6.93, -54.00 8.00))"),
                new GsRegion("antaresubatuba", "polygon((-49.00 -30.00, -46.00 -30.00, -39.00 -23.00, -39.00 -20.00, -40.75 -20.00, -49.00 -24.50, -49.00 -30.00))"),
                new GsRegion("balticsea", "polygon((10.00 54.00,  14.27 53.47,  20.00 54.00, 21.68 54.77, 22.00 56.70, 24.84 56.70, 30.86 60.01, 26.00 62.00, 26.00 66.00, 22.00 66.00, 10.00 60.00, 10.00 54.00))"),
                new GsRegion("beibubay", "polygon((105.00 15.00, 115.00 15.00, 115.00 24.00, 105.00 24.00, 105.00 15.00))"),
                new GsRegion("benguela", "polygon((17.00 -31.00, 17.00 -35.00, 20.00 -35.00, 20.00 -34.67, 18.99 -34.01, 18.50 -33.50, 18.20 -33.00, 18.50 -32.51, 18.50 -32.00, 18.30 -31.50, 17.80 -31.00, 17.00 -31.00))"),
                new GsRegion("capeverde", "polygon((-26.50 23.50, -26.50 13.00, -15.00 13.00, -15.00 23.50, -26.50 23.50))"),
                new GsRegion("centralcalifornia", "polygon((-123.50 36.33, -121.77 36.33, -121.77 38.50, -123.50 38.50, -123.50 36.33))"),
                new GsRegion("chesapeakebay", "polygon((-70.00 34.00, -70.00 41.00, -74.43 40.99, -74.64 40.48, -75.00 40.00, -76.00 40.00, -76.68 39.65, -77.37 39.00, -77.62 38.49, -77.27 36.00, -78.01 34.50, -79.00 34.00, -70.00 34.00))"),
                new GsRegion("chinakoreajapan", "polygon((147.00 25.00, 147.00 45.00, 117.00 45.00, 117.00 25.00, 147.00 25.00))"),
                new GsRegion("dome_c", "polygon((123.20 -77.60, 123.60 -77.60, 123.60 -72.60, 123.20 -72.60, 123.20 -77.60))"),
                new GsRegion("greatbarrierreef", "polygon((141.13 -17.77, 141.71 -17.00, 142.21 -15.06, 142.18 -14.06, 142.52 -12.20, 143.00 -14.04, 143.65 -15.05, 144.55 -15.01, 144.76 -16.51, 145.43 -18.03, 145.75 -19.37, 148.33 -21.02, 149.11 -22.87, 151.75 -25.00, 155.01 -25.01, 155.01 -10.02, 140.03 -10.01, 140.00 -18.10, 141.13 -17.77))"),
                new GsRegion("gulfofmexico", "polygon((-95.00 27.00, -86.00 27.00, -86.00 30.83, -95.00 30.83, -95.00 27.00))"),
                new GsRegion("indonesianwaters", "polygon((94.00 -10.00, 115.00 -10.00, 115.00 8.00, 94.00 8.00, 94.00 -10.00))"),
                new GsRegion("karasea", "polygon((85.00 66.00, 85.00 75.00, 70.00 75.00, 70.00 66.00, 85.00 66.00))"),
                new GsRegion("lakeseriestclair", "polygon((-83.63 41.67, -82.70 41.24, -81.58 41.42, -78.61 42.60,-78.71 43.03, -80.24 42.90, -81.32 42.79, -82.92 42.75, -83.63 41.67))"),
                new GsRegion("lenadelta", "polygon((160.00 70.00, 160.00 76.00, 110.00 76.00, 110.00 70.00, 160.00 70.00))"),
                new GsRegion("mediterranean_blacksea", "polygon((16.77 29.97, 35.80 30.00, 42.27 41.98, 39.63 47.43, 31.50 47.47, 25.95 41.74, 21.20 40.95, 13.85 46.17,  3.50 43.75, -5.50 36.20, -5.50 35.30, 16.77 29.97))"),
                new GsRegion("morocco", "polygon((-26.50 23.50, -15.10 23.50, -5.50 34.00, -5.50 37.00, -8.51 37.50, -8.50 40.00, -26.55 40.00, -26.50 23.50))"),
                new GsRegion("namibianwaters", "polygon((10.00 -29.00, 16.96 -28.98, 15.60 -27.01, 15.30 -26.01, 15.00 -24.00, 14.96 -23.04, 14.68 -22.04, 13.97 -21.04, 13.38 -20.03, 12.85 -19.00, 10.00 -19.00, 10.00 -29.00))"),
                new GsRegion("northsea", "polygon((-19.94 40.00, 0.00 40.00, 0.00 49.22, 12.99 53.99, 13.06 65.00, 0.00 65.00, 0.0 60.00, -20.00 60.00, -19.94 40.00))"),
                new GsRegion("oregon_washington", "polygon((-122.00 42.00, -122.00 51.00, -129.00 51.00, -129.00 49.50, -127.00 49.50, -127.00 42.00, -122.00 42.00))"),
                new GsRegion("puertorico", "polygon((-70.00 11.12, -68.92 10.99, -68.25 10.00, -66.60 10.17, -64.98  9.62, -63.33 10.26, -62.35  9.51, -61.01  8.98, -61.00 20.00, -70.00 20.00, -70.00 11.12))"),
                new GsRegion("redsea", "polygon((32.00 30.00, 32.00 28.80,  39.00 14.99, 43.00 11.00, 44.02 13.00, 43.00 18.00, 39.01 25.00, 36.00 28.00, 35.80 30.00, 32.00 30.00))"),
                new GsRegion("riolaplata", "polygon((-57.01 -41.00, -50.01 -33.03, -50.00 -30.00, -52.03 -30.02, -59.01 -32.98, -63.00 -38.00, -63.04 -41.01, -57.01 -41.00))"),
                new GsRegion("southerncalifornia", "polygon((-117.00 33.78, -119.50 35.50, -122.00 38.00, -126.00 38.00, -126.00 32.50, -117.00 32.50, -117.00 33.78))"),
                new GsRegion("southindia", "polygon((79.33 8.00, 79.35 10.75, 77.01 10.75, 74.87 16.00, 71.00 16.00, 71.00 8.00, 79.33 8.00))"),
                new GsRegion("tasmania", "polygon((143.50 -45.00, 149.50 -45.00, 149.50 -39.50, 143.50 -39.50, 143.50 -45.00))"),
        };
    }
}