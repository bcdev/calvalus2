package com.bc.calvalus.processing.ma;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYErrorRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.Range;
import org.jfree.data.function.LineFunction2D;
import org.jfree.data.general.DatasetUtilities;
import org.jfree.data.statistics.Regression;
import org.jfree.data.xy.DefaultIntervalXYDataset;
import org.jfree.data.xy.IntervalXYDataset;
import org.jfree.data.xy.XYDataset;

import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;

/**
 * Generates plots from the data collected by the {@link PlotDatasetCollector}.
 * It encapsulates JFreeChart API calls, which are used to generate the plots.
 *
 * @author Norman
 */
public class PlotGenerator {

    private int imageWidth;
    private int imageHeight;

    public PlotGenerator() {
        imageWidth = 400;
        imageHeight = 400;
    }

    public void setImageWidth(int imageWidth) {
        this.imageWidth = imageWidth;
    }

    public void setImageHeight(int imageHeight) {
        this.imageHeight = imageHeight;
    }

    public static class Result {
        final PlotDatasetCollector.PlotDataset plotDataset;
        final JFreeChart scatterPlotChart;
        final BufferedImage plotImage;
        final double[] regressionCoefficients;

        public Result(PlotDatasetCollector.PlotDataset plotDataset, JFreeChart scatterPlotChart, BufferedImage plotImage, double[] regressionCoefficients) {
            this.plotDataset = plotDataset;
            this.scatterPlotChart = scatterPlotChart;
            this.plotImage = plotImage;
            this.regressionCoefficients = regressionCoefficients;
        }
    }

    public Result createResult(String title,
                               String subTitle,
                               PlotDatasetCollector.PlotDataset plotDataset) {
        String xTitle = plotDataset.getVariablePair().referenceAttributeName + " in-situ";
        String yTitle = plotDataset.getVariablePair().satelliteAttributeName + " satellite";
        IntervalXYDataset dataset = createDataset(plotDataset);
        double[] regressionCoefficients = {0.0, 1.0};
        if (dataset.getItemCount(0) >= 2) {
            regressionCoefficients = Regression.getOLSRegression(dataset, 0);
        }
        final String subTitle2 = String.format("n=%d, a=%.5f, b=%.5f",
                                               plotDataset.getPoints().length,
                                               regressionCoefficients[0],
                                               regressionCoefficients[1]);
        JFreeChart scatterPlotChart = createChart(title,
                                                  subTitle,
                                                  subTitle2,
                                                  xTitle, yTitle,
                                                  dataset);
        BufferedImage bufferedImage = scatterPlotChart.createBufferedImage(imageWidth, imageHeight);
        return new Result(plotDataset, scatterPlotChart, bufferedImage, regressionCoefficients);
    }

    static JFreeChart createChart(String title,
                                  String subTitle1,
                                  String subTitle2,
                                  PlotDatasetCollector.PlotDataset plotDataset) {
        String xTitle = plotDataset.getVariablePair().referenceAttributeName + " in-situ";
        String yTitle = plotDataset.getVariablePair().satelliteAttributeName + " satellite";
        IntervalXYDataset dataset = createDataset(plotDataset);
        return createChart(title, subTitle1, subTitle2, xTitle, yTitle, dataset);
    }

    static JFreeChart createChart(String title, String subTitle1, String subTitle2, String xTitle, String yTitle, IntervalXYDataset dataset) {
        Font tickLabelFont = new Font("Times New Roman", Font.PLAIN, 16);
        Font labelFont = new Font("Times New Roman", Font.BOLD, 16);

        NumberAxis xAxis = new NumberAxis(xTitle);
        xAxis.setAutoTickUnitSelection(true);
        xAxis.setTickLabelFont(tickLabelFont);
        xAxis.setLabelFont(labelFont);

        NumberAxis yAxis = new NumberAxis(yTitle);
        yAxis.setAutoTickUnitSelection(true);
        yAxis.setTickLabelFont(tickLabelFont);
        yAxis.setLabelFont(labelFont);

        XYErrorRenderer renderer = new XYErrorRenderer();
        renderer.setDrawXError(false);
        renderer.setDrawYError(true);
        renderer.setUseFillPaint(true);
        renderer.setUseOutlinePaint(true);
        renderer.setErrorPaint(Color.DARK_GRAY);
        renderer.setLegendTextFont(0, tickLabelFont);
        renderer.setSeriesShape(0, new Ellipse2D.Double(-4.0, -4.0, 8.0, 8.0));
        renderer.setSeriesPaint(0, Color.BLUE.brighter());
        renderer.setSeriesShapesFilled(0, true);
        renderer.setSeriesLinesVisible(0, false);
        renderer.setSeriesFillPaint(0, Color.BLUE.brighter());
        renderer.setSeriesOutlinePaint(0, Color.BLUE.darker());
        renderer.setSeriesOutlineStroke(0, new BasicStroke(1));

        XYPlot plot = new XYPlot(dataset, xAxis, yAxis, renderer);

        int series = 0;

        Range domainBounds = DatasetUtilities.findDomainBounds(dataset);

        Range xRange = xAxis.getRange();
        Range yRange = yAxis.getRange();
        double min = Math.min(xRange.getLowerBound(), yRange.getLowerBound());
        double max = Math.max(xRange.getUpperBound(), yRange.getUpperBound());
        xAxis.setRangeWithMargins(min, max);
        yAxis.setRangeWithMargins(min, max);

        series++;
        XYDataset identityData = DatasetUtilities.sampleFunction2D(new LineFunction2D(0.0, 1.0),
                                                                   min, max, 2, "Identity");
        plot.setDataset(series, identityData);
        XYLineAndShapeRenderer identityRenderer = new XYLineAndShapeRenderer(true, false);
        identityRenderer.setSeriesPaint(0, Color.GRAY);
        identityRenderer.setSeriesStroke(0, new BasicStroke(1, 0, 0, 10f, new float[]{4f, 4f}, 0f));
        plot.setRenderer(series, identityRenderer);

        if (dataset.getItemCount(0) >= 2) {
            series++;
            double[] coefficients = Regression.getOLSRegression(dataset, 0);
            if (domainBounds.getLowerBound() < domainBounds.getUpperBound()) {
                XYDataset regressionData = DatasetUtilities.sampleFunction2D(new LineFunction2D(coefficients[0], coefficients[1]),
                                                                             domainBounds.getLowerBound(),
                                                                             domainBounds.getUpperBound(),
                                                                             2, "Regression");
                plot.setDataset(series, regressionData);
                XYLineAndShapeRenderer regressionRenderer = new XYLineAndShapeRenderer(true, false);
                regressionRenderer.setSeriesPaint(0, Color.DARK_GRAY);
                plot.setRenderer(series, regressionRenderer);
            }
        }

        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, false);
        if (subTitle1 != null) {
            chart.addSubtitle(new TextTitle(subTitle1));
        }
        if (subTitle2 != null) {
            chart.addSubtitle(new TextTitle(subTitle2));
        }
        chart.setTextAntiAlias(true);
        chart.setAntiAlias(true);
        return chart;
    }

    private static IntervalXYDataset createDataset(PlotDatasetCollector.PlotDataset plotDataset) {
        PlotDatasetCollector.Point[] points = plotDataset.getPoints();
        DefaultIntervalXYDataset dataset = new DefaultIntervalXYDataset();
        int n = points.length;
        double[][] data = new double[6][n];
        double[] xData = data[0];
        double[] xsData = data[1];
        double[] xeData = data[2];
        double[] yData = data[3];
        double[] ysData = data[4];
        double[] yeData = data[5];
        for (int i = 0; i < n; i++) {
            PlotDatasetCollector.Point point = points[i];
            xData[i] = point.referenceValue;
            yData[i] = point.satelliteMean;
            xsData[i] = xeData[i] = xData[i];
            ysData[i] = yData[i] - point.satelliteSigma;
            yeData[i] = yData[i] + point.satelliteSigma;
        }
        dataset.addSeries(plotDataset.getVariablePair().referenceAttributeName + "/" + plotDataset.getGroupName(), data);
        return dataset;
    }


}
