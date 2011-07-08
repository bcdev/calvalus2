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
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

    public JFreeChart createGRaph(String regionName, int aggregatorIndex, int varIndex, int vectorIndex) {
        XYDataset dataset = createDataset(regionName, vectorIndex);
        String columnName = taResult.getOutputFeatureNames(aggregatorIndex)[varIndex];
        return createChart(dataset, regionName, columnName);
    }

    public static void writeChart(JFreeChart chart, OutputStream outputStream) {
        BufferedImage bufferedImage = chart.createBufferedImage(800, 400);
        try {
            ImageIO.write(bufferedImage, "PNG", outputStream);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private JFreeChart createChart(XYDataset dataset, String regionName, String columnName) {

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "TimeSeries for " + regionName,  // title
                "Time",        // x-axis label
                columnName,    // y-axis label
                dataset,       // data
                true,          // create legend?
                true,          // generate tooltips?
                false          // generate URLs?
        );

        chart.setBackgroundPaint(Color.white);

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.lightGray);
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);
        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
        plot.setDomainCrosshairVisible(true);
        plot.setRangeCrosshairVisible(true);

        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("dd-MMM"));
        return chart;
    }

    private XYDataset createDataset(String regionName, int vectorIndex) {
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        Map<Integer, TimeSeries> yearlyTimeSeries = new HashMap<Integer, TimeSeries>();

        List<TAResult.Record> records = taResult.getRecords(regionName);
        Calendar calendar = ProductData.UTC.createCalendar();
        for (TAResult.Record record : records) {
            try {
                Date date = getCenterDate(record);
                calendar.setTime(date);
                int year = calendar.get(Calendar.YEAR);
                TimeSeries ts = getTimeSeries(yearlyTimeSeries, year);
                double sample = record.outputVector.get(vectorIndex); //mean

                calendar.set(Calendar.YEAR, 2000);
                ts.add(new Day(calendar.getTime(), ProductData.UTC.UTC_TIME_ZONE, Locale.ENGLISH), sample);
            } catch (ParseException e) {
                e.printStackTrace();  //ignore ths record
            }
        }
        for (TimeSeries timeSeries : yearlyTimeSeries.values()) {
            dataset.addSeries(timeSeries);
        }
        return dataset;
    }

    private TimeSeries getTimeSeries(Map<Integer, TimeSeries> yearlyTimeSeries, int year) {
        TimeSeries timeSeries = yearlyTimeSeries.get(year);
        if (timeSeries == null) {
            timeSeries = new TimeSeries(Integer.toString(year));
            yearlyTimeSeries.put(year, timeSeries);
        }
        return timeSeries;
    }

    private Date getCenterDate(TAResult.Record record) throws ParseException {
        Date date1 = DATE_FORMAT.parse(record.startDate);
        Date date2 = DATE_FORMAT.parse(record.stopDate);
        long t = (date2.getTime() - date1.getTime()) / 2;
        return new Date(date1.getTime() + t);
    }


    public static void main(String[] args) throws IOException {
        TAResult taResult = new TAResult(1);
        taResult.setOutputFeatureNames(0, new String[]{"chl_conc_mean", "chl_conc_stdev"});
        taResult.addRecord("balticsea", "2008-06-04", "2008-06-06", new VectorImpl(new float[]{0.3F, 0.1F}));
        taResult.addRecord("northsea", "2008-06-07", "2008-06-09", new VectorImpl(new float[]{0.8F, 0.2F}));
        taResult.addRecord("balticsea", "2008-06-07", "2008-06-09", new VectorImpl(new float[]{0.1F, 0.2F}));
        taResult.addRecord("northsea", "2008-06-04", "2008-06-06", new VectorImpl(new float[]{0.4F, 0.0F}));
        taResult.addRecord("balticsea", "2008-06-01", "2008-06-03", new VectorImpl(new float[]{0.8F, 0.0F}));
        taResult.addRecord("northsea", "2008-06-01", "2008-06-03", new VectorImpl(new float[]{0.3F, 0.1F}));
        taResult.addRecord("northsea", "2008-06-13", "2008-06-15", new VectorImpl(new float[]{0.4F, 0.4F}));
        taResult.addRecord("balticsea", "2008-06-10", "2008-06-12", new VectorImpl(new float[]{0.6F, 0.3F}));
        taResult.addRecord("northsea", "2008-06-10", "2008-06-12", new VectorImpl(new float[]{0.2F, 0.1F}));

        taResult.addRecord("northsea", "2009-06-07", "2009-06-09", new VectorImpl(new float[]{0.7F, 0.2F}));
        taResult.addRecord("northsea", "2009-06-04", "2009-06-06", new VectorImpl(new float[]{0.5F, 0.0F}));
        taResult.addRecord("northsea", "2009-06-01", "2009-06-03", new VectorImpl(new float[]{0.2F, 0.1F}));
        taResult.addRecord("northsea", "2009-06-13", "2009-06-15", new VectorImpl(new float[]{0.4F, 0.4F}));
        taResult.addRecord("northsea", "2009-06-10", "2009-06-12", new VectorImpl(new float[]{0.3F, 0.1F}));

        taResult.addRecord("northsea", "2007-06-07", "2007-06-09", new VectorImpl(new float[]{0.9F, 0.2F}));
        taResult.addRecord("northsea", "2007-06-04", "2007-06-06", new VectorImpl(new float[]{0.5F, 0.0F}));
        taResult.addRecord("northsea", "2007-06-01", "2007-06-03", new VectorImpl(new float[]{0.3F, 0.1F}));
        taResult.addRecord("northsea", "2007-06-13", "2007-06-15", new VectorImpl(new float[]{0.2F, 0.4F}));
        taResult.addRecord("northsea", "2007-06-10", "2007-06-12", new VectorImpl(new float[]{0.3F, 0.1F}));

        TAGraph taGraph = new TAGraph(taResult);
        TAReport taReport = new TAReport(taResult);
        Set<String> regionNames = taResult.getRegionNames();

        for (String regionName : regionNames) {
            int vectorIndex = 0;
            for (int aggregatorIndex = 0; aggregatorIndex < taResult.getAggregatorCount(); aggregatorIndex++) {
                String[] outputFeatureNames = taResult.getOutputFeatureNames(aggregatorIndex);
                for (int i = 0; i < outputFeatureNames.length; i++) {
                    File pngFile = new File("/tmp/" + regionName + "_" + outputFeatureNames[i] + ".png");
                    JFreeChart chart = taGraph.createGRaph(regionName, aggregatorIndex, i, vectorIndex);
                    TAGraph.writeChart(chart, new FileOutputStream(pngFile));

                    vectorIndex++;
                }
            }
            File csvFile = new File("/tmp/" + regionName + ".csv");
            taReport.writeRegionCsvReport(new FileWriter(csvFile), regionName);
        }
    }
}
