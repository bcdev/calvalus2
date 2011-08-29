package com.bc.calvalus.processing.ma;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
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

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * Generates plots from the data collected by the {@link PlotDatasetCollector}.
 *
 * @author Norman
 */
public class PlotGenerator {


    public static JFreeChart createScatterPlotChart(String title, String subTitle, String labelX, String labelY, XYDataset dataset) {
        if (PlotOrientation.VERTICAL == null) {
            throw new IllegalArgumentException("Null 'orientation' argument.");
        }

        Font tickLabelFont = new Font("Times New Roman", Font.PLAIN, 16);
        Font labelFont = new Font("Times New Roman", Font.BOLD, 16);

        NumberAxis xAxis = new NumberAxis(labelX);
        xAxis.setAutoTickUnitSelection(true);
        xAxis.setTickLabelFont(tickLabelFont);
        xAxis.setLabelFont(labelFont);

        NumberAxis yAxis = new NumberAxis(labelY);
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

        series++;
        double[] coefficients = Regression.getOLSRegression(dataset, 0);
        XYDataset regressionData = DatasetUtilities.sampleFunction2D(new LineFunction2D(coefficients[0], coefficients[1]),
                                                                     domainBounds.getLowerBound(), domainBounds.getUpperBound(), 2, "Regression");
        plot.setDataset(series, regressionData);
        XYLineAndShapeRenderer regressionRenderer = new XYLineAndShapeRenderer(true, false);
        regressionRenderer.setSeriesPaint(0, Color.DARK_GRAY);
        plot.setRenderer(series, regressionRenderer);

        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, false);
        chart.addSubtitle(new TextTitle(subTitle));
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


    public void writeScatterPlotImage(PlotDatasetCollector.PlotDataset plotDataset, String name) throws IOException {
        IntervalXYDataset dataset = createDataset(plotDataset);
        JFreeChart scatterPlotChart = createScatterPlotChart(plotDataset.getVariablePair().referenceAttributeName + "/" + plotDataset.getGroupName(),
                                                             name,
                                                             plotDataset.getVariablePair().referenceAttributeName,
                                                             plotDataset.getVariablePair().satelliteAttributeName,
                                                             dataset);

        final BufferedImage bufferedImage = scatterPlotChart.createBufferedImage(400, 400);
        ImageIO.write(bufferedImage, "PNG", new File(name + ".png"));

    }
}
