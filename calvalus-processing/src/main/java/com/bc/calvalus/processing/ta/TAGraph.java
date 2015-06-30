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

import org.esa.snap.binning.support.VectorImpl;
import org.esa.snap.framework.datamodel.ProductData;
import org.esa.snap.util.io.CsvReader;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickUnit;
import org.jfree.chart.axis.DateTickUnitType;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYDifferenceRenderer;
import org.jfree.chart.renderer.xy.XYErrorRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.function.Function2D;
import org.jfree.data.function.LineFunction2D;
import org.jfree.data.time.Day;
import org.jfree.data.time.MovingAverage;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.data.xy.YIntervalSeries;
import org.jfree.data.xy.YIntervalSeriesCollection;
import org.jfree.ui.RectangleInsets;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
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

    public JFreeChart createAnomalyGraph(String regionName, int featureIndex) {
        List<TAResult.Record> records = taResult.getRecords(regionName);
        YIntervalSeriesCollection dataset = createAnomalyDataset(records, featureIndex);
        String featureName = taResult.getOutputFeatureNames()[featureIndex];

        DateAxis dateAxis = new DateAxis("Time");
        NumberAxis numberaxis1 = new NumberAxis(featureName);
        XYErrorRenderer xyerrorrenderer = new XYErrorRenderer();
        xyerrorrenderer.setBaseLinesVisible(false);
        xyerrorrenderer.setBaseShapesVisible(false);
        xyerrorrenderer.setErrorPaint(Color.BLACK);
        XYPlot plot = new XYPlot(dataset, dateAxis, numberaxis1, xyerrorrenderer);


        configurePlot(plot);
        long averagingPeriod = 1000 * 60 * 60 * 24 * 300L; // 300 days
        XYDataset movingAverage = MovingAverage.createMovingAverage(dataset, "moving_average", averagingPeriod, 0);
        plot.setDataset(1, movingAverage);
        XYLineAndShapeRenderer movingAverageRenderer = new XYLineAndShapeRenderer(true, false);
        movingAverageRenderer.setSeriesPaint(0, Color.BLUE);
        plot.setRenderer(1, movingAverageRenderer);

        if (dataset.getItemCount(0) >= 2) {
            XYDataset minMax = getMinMax(dataset, 0, 2);
            plot.setDataset(2, minMax);

            Color lightGray = new Color(Color.LIGHT_GRAY.getRed(), Color.LIGHT_GRAY.getGreen(), Color.LIGHT_GRAY.getGreen(), 128);
            XYDifferenceRenderer xyDifferenceRenderer = new XYDifferenceRenderer(lightGray, Color.red, false);
            xyDifferenceRenderer.setSeriesPaint(0, Color.LIGHT_GRAY);
            xyDifferenceRenderer.setSeriesPaint(1, Color.LIGHT_GRAY);
            plot.setRenderer(2, xyDifferenceRenderer);
        }

        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("MMM-yyyy"));
        axis.setTickUnit(new DateTickUnit(DateTickUnitType.YEAR, 1));

        return new JFreeChart("Anomaly for " + regionName, JFreeChart.DEFAULT_TITLE_FONT, plot, false);
    }

    public JFreeChart createYearlyCyclGaph(String regionName, int featureIndex) {
        List<TAResult.Record> records = taResult.getRecords(regionName);
        XYDataset dataset = createYearlyCycleDataset(records, featureIndex);
        String featureName = taResult.getOutputFeatureNames()[featureIndex];
        JFreeChart chart = createChart("Yearly-Cycle for " + regionName, dataset, featureName, true);

        XYPlot plot = (XYPlot) chart.getPlot();
        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("MMM"));
        axis.setTickUnit(new DateTickUnit(DateTickUnitType.MONTH, 3));

        return chart;
    }

    public JFreeChart createTimeseriesGaph(String regionName, int featureIndex) {
        List<TAResult.Record> records = taResult.getRecords(regionName);
        XYDataset dataset = createTimeseriesDataset(records, featureIndex);
        String featureName = taResult.getOutputFeatureNames()[featureIndex];
        JFreeChart chart = createChart("Timeseries for " + regionName, dataset, featureName, true);

        XYPlot plot = (XYPlot) chart.getPlot();
        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("MMM-yyyy"));

        return chart;
    }

    public JFreeChart createTimeseriesSigmaGraph(String regionName, int featureIndex, int sigmaIndex) {

        String featureName = taResult.getOutputFeatureNames()[featureIndex];
        List<TAResult.Record> records = taResult.getRecords(regionName);

        XYDataset dataset1 = createTimeseriesDataset(records, featureIndex);
        YIntervalSeriesCollection dataset2 = createSigmaDataset(records, featureIndex, sigmaIndex);

        DateAxis dateAxis = new DateAxis("Time");
        dateAxis.setDateFormatOverride(new SimpleDateFormat("MMM-yyyy"));
        NumberAxis numberAxis = new NumberAxis(featureName);

        XYLineAndShapeRenderer lineRenderer = new XYLineAndShapeRenderer(true, false);
        lineRenderer.setSeriesPaint(0, Color.BLUE);

        XYErrorRenderer xyerrorrenderer = new XYErrorRenderer();
        xyerrorrenderer.setBaseLinesVisible(false);
        xyerrorrenderer.setBaseShapesVisible(false);
        xyerrorrenderer.setErrorPaint(Color.BLACK);

        XYPlot plot = new XYPlot(dataset1, dateAxis, numberAxis, lineRenderer);
        plot.setRenderer(1, xyerrorrenderer);
        plot.setDataset(1, dataset2);

        return new JFreeChart("Timeseries for " + regionName, JFreeChart.DEFAULT_TITLE_FONT, plot, false);
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
        XYPlot plot = (XYPlot) chart.getPlot();
        configurePlot(plot);
        return chart;
    }

    private static void configurePlot(XYPlot plot) {
        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
        plot.setDomainCrosshairVisible(true);
        plot.setRangeCrosshairVisible(true);
    }

    private YIntervalSeriesCollection createAnomalyDataset(List<TAResult.Record> records, int featureIndex) {
        Map<Integer, List<Double>> values = new HashMap<Integer, List<Double>>();
        Calendar calendar = ProductData.UTC.createCalendar();
        for (TAResult.Record record : records) {
            try {
                Date date = getCenterDate(record);
                calendar.setTime(date);
                int month = calendar.get(Calendar.MONTH);
                List<Double> doubleList = values.get(month);
                if (doubleList == null) {
                    doubleList = new ArrayList<Double>();
                    values.put(month, doubleList);
                }
                double sample = record.outputVector.get(featureIndex);
                doubleList.add(sample);
            } catch (ParseException e) {
                e.printStackTrace();  //ignore this record
            }
        }
        Map<Integer, Double> means = new HashMap<Integer, Double>();
        Map<Integer, Double> sigmas = new HashMap<Integer, Double>();
        for (Integer month : values.keySet()) {
            double sum = 0;
            int count = 0;
            List<Double> doubleList = values.get(month);
            for (Double value : doubleList) {
                sum += value;
                count++;
            }
            final double mean = count > 0 ? sum / count : Double.NaN;
            means.put(month, mean);
            double sumSigma = 0;
            for (Double value : doubleList) {
                sumSigma += (mean - value) * (mean - value);
            }
            final double sigma = count > 1 ? Math.sqrt(sumSigma / (count - 1)) : 0.0;
            sigmas.put(month, sigma);
        }

        YIntervalSeries yintervalseries = new YIntervalSeries("Series 1");
        for (TAResult.Record record : records) {
            try {
                Date date = getCenterDate(record);
                calendar.setTime(date);
                int month = calendar.get(Calendar.MONTH);
                double sample = record.outputVector.get(featureIndex);

                double mean = means.get(month);
                double sigma = sigmas.get(month);
                double y = sample - mean;

                yintervalseries.add(date.getTime(), y, y - sigma, y + sigma);
            } catch (ParseException e) {
                e.printStackTrace();  //ignore this record
            }
        }
        YIntervalSeriesCollection yIntervalSeriesCollection = new YIntervalSeriesCollection();
        yIntervalSeriesCollection.addSeries(yintervalseries);
        return yIntervalSeriesCollection;
    }

    private XYDataset createYearlyCycleDataset(List<TAResult.Record> records, int featureIndex) {
        Map<Integer, TimeSeries> yearlyTimeSeries = new HashMap<Integer, TimeSeries>();
        Calendar calendar = ProductData.UTC.createCalendar();
        for (TAResult.Record record : records) {
            try {
                Date date = getCenterDate(record);
                calendar.setTime(date);
                int year = calendar.get(Calendar.YEAR);
                TimeSeries ts = getTimeSeries(yearlyTimeSeries, year);
                calendar.set(Calendar.YEAR, 2000);
                //Month time = new Month(calendar.getTime(), ProductData.UTC.UTC_TIME_ZONE, Locale.ENGLISH);
                //allow also shorter sampling periods than one month, down to one day
                Day time = new Day(calendar.getTime(), ProductData.UTC.UTC_TIME_ZONE, Locale.ENGLISH);
                double sample = record.outputVector.get(featureIndex);
                ts.add(time, sample);
            } catch (ParseException e) {
                e.printStackTrace();  //ignore this record
            }
        }
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        List<Integer> yearList = new ArrayList<Integer>(yearlyTimeSeries.keySet());
        Collections.sort(yearList);
        for (Integer year : yearList) {
            dataset.addSeries(yearlyTimeSeries.get(year));
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
                //Month time = new Month(calendar.getTime(), ProductData.UTC.UTC_TIME_ZONE, Locale.ENGLISH);
                //allow also shorter sampling periods than one month, down to one day
                Day time = new Day(calendar.getTime(), ProductData.UTC.UTC_TIME_ZONE, Locale.ENGLISH);
                double sample = record.outputVector.get(featureIndex);
                ts.add(time, sample);
            } catch (ParseException e) {
                e.printStackTrace();  //ignore this record
            }
        }
        return new TimeSeriesCollection(ts, ProductData.UTC.UTC_TIME_ZONE);
    }

    private YIntervalSeriesCollection createSigmaDataset(List<TAResult.Record> records, int featureIndex, int sigmaIndex) {
        YIntervalSeries yintervalseries = new YIntervalSeries("Sigma");
        for (TAResult.Record record : records) {
            try {
                long time = getCenterDate(record).getTime();
                double mean = record.outputVector.get(featureIndex);
                double sigma = record.outputVector.get(sigmaIndex);
                yintervalseries.add(time, mean, mean - sigma, mean + sigma);
            } catch (ParseException e) {
                e.printStackTrace();  //ignore this record
            }
        }
        YIntervalSeriesCollection yIntervalSeriesCollection = new YIntervalSeriesCollection();
        yIntervalSeriesCollection.addSeries(yintervalseries);
        return yIntervalSeriesCollection;
    }

    private static synchronized Date getCenterDate(TAResult.Record record) throws ParseException {
        Date date1 = DATE_FORMAT.parse(record.startDate);
        Date date2 = DATE_FORMAT.parse(record.stopDate);
        long t = (date2.getTime() - date1.getTime()) / 2;
        return new Date(date1.getTime() + t);
    }

    static double[] getRegressionWithSigma(XYDataset data, int series) {

        int n = data.getItemCount(series);
        if (n < 2) {
            throw new IllegalArgumentException("Not enough data.");
        }

        double sumX = 0;
        double sumY = 0;
        for (int i = 0; i < n; i++) {
            double x = data.getXValue(series, i);
            double y = data.getYValue(series, i);

            sumX += x;
            sumY += y;
        }

        double xMean = sumX / n;
        double sumT2 = 0;
        double sumB = 0;
        for (int i = 0; i < n; i++) {
            double x = data.getXValue(series, i);
            double y = data.getYValue(series, i);

            double t = x - xMean;
            sumT2 += t * t;
            sumB += t * y;
        }

        double b = sumB / sumT2;
        double a = (sumY - sumX * b) / n;
        double sdeva = Math.sqrt((1.0 + sumX * sumX / (n * sumT2)) / n);
        double sdevb = Math.sqrt(1.0 / sumT2);

        double sumChiSqr = 0.0;
        for (int i = 0; i < n; i++) {
            double x = data.getXValue(series, i);
            double y = data.getYValue(series, i);

            double chi = y - a - b * x;
            sumChiSqr += chi * chi;
        }

        double sdevdat = Math.sqrt(sumChiSqr / (n - 2));
        sdeva = sdeva * sdevdat;
        sdevb = sdevb * sdevdat;

        return new double[]{a, b, sdeva, sdevb};
    }

    static XYDataset getMinMax(XYDataset data, int series, int distance) {
        XYSeriesCollection seriesCollection = new XYSeriesCollection();
        XYSeries mins = new XYSeries("min");
        XYSeries maxs = new XYSeries("max");
        seriesCollection.addSeries(maxs);
        seriesCollection.addSeries(mins);

        double[] coeffs = getRegressionWithSigma(data, 0);
        Function2D[] funcs = new Function2D[]{
                new LineFunction2D(coeffs[0] + distance * coeffs[2], coeffs[1] + distance * coeffs[3]),
                new LineFunction2D(coeffs[0] + distance * coeffs[2], coeffs[1] - distance * coeffs[3]),
                new LineFunction2D(coeffs[0] - distance * coeffs[2], coeffs[1] + distance * coeffs[3]),
                new LineFunction2D(coeffs[0] - distance * coeffs[2], coeffs[1] - distance * coeffs[3])
        };

        int n = data.getItemCount(series);
        for (int i = 0; i < n; i++) {
            double x =  data.getXValue(series, i);
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            for (Function2D func : funcs) {
                double y = func.getValue(x);
                min = Math.min(min, y);
                max = Math.max(max, y);
            }
            mins.add(x, min);
            maxs.add(x, max);
        }
        return seriesCollection;
    }


    public static void main(String[] args) throws IOException {
        InputStream inputStream;
        if (args.length == 1) {
            inputStream = new FileInputStream(args[0]);
        } else {
            inputStream = TAGraph.class.getResourceAsStream("oc_cci.South_Pacific_Gyre.csv");
        }
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        CsvReader csvReader = new CsvReader(inputStreamReader, new char[]{'\t'});
        List<String[]> stringRecords = csvReader.readStringRecords();
        TAResult taResult = new TAResult();
        String[] header = stringRecords.get(0);
        taResult.setOutputFeatureNames(Arrays.copyOfRange(header, 2, header.length));
        for (int i = 1; i < stringRecords.size(); i++) {
            String[] strings = stringRecords.get(i);
            float[] elements = new float[strings.length - 2];
            for (int f = 0; f < elements.length; f++) {
                elements[f] = Float.parseFloat(strings[f + 2]);
            }
            VectorImpl outputVector = new VectorImpl(elements);
            taResult.addRecord("Example", strings[0], strings[1], outputVector);
        }

        TAGraph taGraph = new TAGraph(taResult);
        Set<String> regionNames = taResult.getRegionNames();

        String[] outputFeatureNames = taResult.getOutputFeatureNames();
        for (String regionName : regionNames) {
            for (int i = 0; i < outputFeatureNames.length; i++) {
                File pngFile;
                JFreeChart chart;
                pngFile = new File("/tmp/Yearly_cycle-" + regionName + "-" + outputFeatureNames[i] + ".png");
                chart = taGraph.createYearlyCyclGaph(regionName, i);
                TAGraph.writeChart(chart, new FileOutputStream(pngFile));

                pngFile = new File("/tmp/Timeseries-" + regionName + "-" + outputFeatureNames[i] + ".png");
                chart = taGraph.createTimeseriesGaph(regionName, i);
                TAGraph.writeChart(chart, new FileOutputStream(pngFile));

                pngFile = new File("/tmp/Anomaly-" + regionName + "-" + outputFeatureNames[i] + ".png");
                chart = taGraph.createAnomalyGraph(regionName, i);
                TAGraph.writeChart(chart, new FileOutputStream(pngFile));
            }
        }
    }
}
