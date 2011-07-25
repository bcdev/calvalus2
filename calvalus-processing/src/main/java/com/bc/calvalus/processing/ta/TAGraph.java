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

package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.VectorImpl;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.io.CsvReader;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.time.Day;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.RectangleInsets;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TAGraph {

    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat(DATE_PATTERN);

    private final TAResult taResult;

    public TAGraph(TAResult taResult) {
        this.taResult = taResult;
    }

    public JFreeChart createYearlyCyclGaph(String regionName, int featureIndex) {
        List<TAResult.Record> records = taResult.getRecords(regionName);
        XYDataset dataset = createYearlyCycleDataset(records, featureIndex);
        String featureName = taResult.getOutputFeatureNames()[featureIndex];
        JFreeChart chart = createChart("Yearly-Cycle for " + regionName, dataset, featureName, true);

        XYPlot plot = (XYPlot) chart.getPlot();
        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("MMM"));

        return chart;
    }

    public JFreeChart createTimeseriesGaph(String regionName, int featureIndex) {
        List<TAResult.Record> records = taResult.getRecords(regionName);
        XYDataset dataset = createTimeseriesDataset(records, featureIndex);
        String featureName = taResult.getOutputFeatureNames()[featureIndex];
        JFreeChart chart = createChart("Timeseries for " + regionName, dataset, featureName, false);

        XYPlot plot = (XYPlot) chart.getPlot();
        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("MMM-yyyy"));

        return chart;
    }

    public static void writeChart(JFreeChart chart, OutputStream outputStream) throws IOException {
        BufferedImage bufferedImage = chart.createBufferedImage(800, 400);
        ImageIO.write(bufferedImage, "PNG", outputStream);
    }

    private static JFreeChart createChart(String title, XYDataset dataset, String featureName, boolean legend) {

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                title,  // title
                "Time",        // x-axis label
                featureName,    // y-axis label
                dataset,       // data
                legend,          // create legend?
                false,          // generate tooltips?
                false          // generate URLs?
        );
        chart.setBackgroundPaint(Color.white);
        XYPlot plot = (XYPlot) chart.getPlot();
        configurePlot(plot);
        return chart;
    }

    private static void configurePlot(XYPlot plot) {
        plot.setBackgroundPaint(Color.lightGray);
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);
        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
        plot.setDomainCrosshairVisible(true);
        plot.setRangeCrosshairVisible(true);
    }


    private XYDataset createYearlyCycleDataset(List<TAResult.Record> records, int featureIndex) {
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        Map<Integer, TimeSeries> yearlyTimeSeries = new HashMap<Integer, TimeSeries>();

        Calendar calendar = ProductData.UTC.createCalendar();
        for (TAResult.Record record : records) {
            try {
                Date date = getCenterDate(record);
                calendar.setTime(date);
                int year = calendar.get(Calendar.YEAR);
                TimeSeries ts = getTimeSeries(yearlyTimeSeries, year);
                calendar.set(Calendar.YEAR, 2000);
                Day day = new Day(calendar.getTime(), ProductData.UTC.UTC_TIME_ZONE, Locale.ENGLISH);
                double sample = record.outputVector.get(featureIndex);
                ts.add(day, sample);
            } catch (ParseException e) {
                e.printStackTrace();  //ignore this record
            }
        }
        for (TimeSeries timeSeries : yearlyTimeSeries.values()) {
            dataset.addSeries(timeSeries);
        }
        return dataset;
    }

    private static TimeSeries getTimeSeries(Map<Integer, TimeSeries> yearlyTimeSeries, int year) {
        TimeSeries timeSeries = yearlyTimeSeries.get(year);
        if (timeSeries == null) {
            timeSeries = new TimeSeries(Integer.toString(year));
            yearlyTimeSeries.put(year, timeSeries);
        }
        return timeSeries;
    }

    private XYDataset createTimeseriesDataset(List<TAResult.Record> records, int featureIndex) {
         String featureName = taResult.getOutputFeatureNames()[featureIndex];
        TimeSeries ts = new TimeSeries(featureName);

        Calendar calendar = ProductData.UTC.createCalendar();
        for (TAResult.Record record : records) {
            try {
                Date date = getCenterDate(record);
                calendar.setTime(date);
                Day day = new Day(calendar.getTime(), ProductData.UTC.UTC_TIME_ZONE, Locale.ENGLISH);
                double sample = record.outputVector.get(featureIndex);
                ts.add(day, sample);
            } catch (ParseException e) {
                e.printStackTrace();  //ignore this record
            }
        }
        return new TimeSeriesCollection(ts);
    }

    private static Date getCenterDate(TAResult.Record record) throws ParseException {
        Date date1 = DATE_FORMAT.parse(record.startDate);
        Date date2 = DATE_FORMAT.parse(record.stopDate);
        long t = (date2.getTime() - date1.getTime()) / 2;
        return new Date(date1.getTime() + t);
    }


    public static void main(String[] args) throws IOException {
        InputStream resourceAsStream = TAGraph.class.getResourceAsStream("oc_cci.South_Pacific_Gyre.csv");
        InputStreamReader inputStreamReader = new InputStreamReader(resourceAsStream);
        CsvReader csvReader = new CsvReader(inputStreamReader, new char[]{'\t'});
        List<String[]> stringRecords = csvReader.readStringRecords();
        TAResult taResult = new TAResult();
        String[] header = stringRecords.get(0);
        taResult.setOutputFeatureNames(Arrays.copyOfRange(header, 2, header.length));
        for (int i = 1; i < stringRecords.size(); i++) {
            String[] strings = stringRecords.get(i);
            float[] elements = new float[strings.length-2];
            for (int f = 0; f < elements.length; f++) {
                elements[f] = Float.parseFloat(strings[f+2]);
            }
            VectorImpl outputVector = new VectorImpl(elements);
            taResult.addRecord("oc_cci.South_Pacific_Gyre", strings[0], strings[1], outputVector);
        }

        TAGraph taGraph = new TAGraph(taResult);
        Set<String> regionNames = taResult.getRegionNames();

        String[] outputFeatureNames = taResult.getOutputFeatureNames();
        for (String regionName : regionNames) {
            for (int i = 0; i < outputFeatureNames.length; i++) {
                File pngFile = new File("/tmp/Yearly_cycle-" + regionName + "-" + outputFeatureNames[i] + ".png");
                JFreeChart chart = taGraph.createYearlyCyclGaph(regionName, i);
                TAGraph.writeChart(chart, new FileOutputStream(pngFile));

                pngFile = new File("/tmp/Timeseries-" + regionName + "-" + outputFeatureNames[i] + ".png");
                chart = taGraph.createTimeseriesGaph(regionName, i);
                TAGraph.writeChart(chart, new FileOutputStream(pngFile));

            }
        }
    }
}
