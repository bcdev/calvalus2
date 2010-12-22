/* --------------------
 * RegressionDemo1.java
 * --------------------
 * (C) Copyright 2002-2008, by Object Refinery Limited.
 *
 */

package sandbox.regressionPlots;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.Range;
import org.jfree.data.function.Function2D;
import org.jfree.data.function.LineFunction2D;
import org.jfree.data.general.DatasetUtilities;
import org.jfree.data.statistics.Regression;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import sandbox.util.Utils;

import java.awt.*;

/**
 * A demo showing one way to fit regression lines to XY data.
 */
public class Example_RegressionPlotLinearTime {
    private XYDataset data;
    private int datasetIndex = 0;

    public static void main(String args[]) {
        final Example_RegressionPlotLinearTime regressionPlotLinear = new Example_RegressionPlotLinearTime();
        regressionPlotLinear.data = regressionPlotLinear.createTimSeriesDataset();
        Utils.saveChartOnScreen(600, 600, regressionPlotLinear.createChart());
    }

    private JFreeChart createChart() {

        ValueAxis xAxis = new DateAxis("X"); //DateAxis makes the time
        NumberAxis yAxis = new NumberAxis("Y");

        final XYPlot plot = new XYPlot(null, xAxis, yAxis, null);

        //dataset 1
        plot.setDataset(datasetIndex++, data);
        final boolean lines1 = false;
        final boolean shapes1 = true;

        XYLineAndShapeRenderer renderer1 = new XYLineAndShapeRenderer(lines1, shapes1);
        renderer1.setSeriesPaint(0, Color.BLACK);
        plot.setRenderer(0, renderer1);

        //regression functions calculation
        double[] coefficients = Regression.getOLSRegression(data, 0);
        double[] coefficients2 = Regression.getOLSRegression(data, 1);
        Function2D formula = new LineFunction2D(coefficients[0], coefficients[1]);
        Function2D formula2 = new LineFunction2D(coefficients2[0], coefficients2[1]);

        //dataset 2
        final Range range = DatasetUtilities.findDomainBounds(data);
        XYDataset regressionData = DatasetUtilities.sampleFunction2D(
                formula, range.getLowerBound(), range.getUpperBound(), 100, "Regression Line Set 1");
        plot.setDataset(datasetIndex++, regressionData);
        //dataset 3
        XYDataset regressionData2 = DatasetUtilities.sampleFunction2D(
                formula2, range.getLowerBound(), range.getUpperBound(), 100, "Regression Line Set 2");
        plot.setDataset(datasetIndex++, regressionData2);
        //renderer 2
        final boolean lines2 = true;
        final boolean shapes2 = false;
        XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer(lines2, shapes2);
        renderer2.setSeriesPaint(0, Color.BLACK);
        plot.setRenderer(1, renderer2);
        //renderer 3
        XYLineAndShapeRenderer renderer3 = new XYLineAndShapeRenderer(lines2, shapes2);
        renderer3.setSeriesPaint(0, Color.BLUE);
        plot.setRenderer(2, renderer3);

        return new JFreeChart("Linear Regression of Time Series", JFreeChart.DEFAULT_TITLE_FONT, plot, true);
    }

    private XYDataset createTimSeriesDataset() {
        TimeSeries s1 = new TimeSeries("L&G European Index Trust");
        s1.add(new Month(2, 2001), 181.8);
        s1.add(new Month(3, 2001), 167.3);
        s1.add(new Month(4, 2001), 153.8);
        s1.add(new Month(5, 2001), 167.6);
        s1.add(new Month(6, 2001), 158.8);
        s1.add(new Month(7, 2001), 148.3);
        s1.add(new Month(8, 2001), 153.9);
        s1.add(new Month(9, 2001), 142.7);
        s1.add(new Month(10, 2001), 123.2);
        s1.add(new Month(11, 2001), 131.8);
        s1.add(new Month(12, 2001), 139.6);
        s1.add(new Month(1, 2002), 142.9);
        s1.add(new Month(2, 2002), 138.7);
        s1.add(new Month(3, 2002), 137.3);
        s1.add(new Month(4, 2002), 143.9);
        s1.add(new Month(5, 2002), 139.8);
        s1.add(new Month(6, 2002), 137.0);
        s1.add(new Month(7, 2002), 132.8);
        TimeSeries s2 = new TimeSeries("L&G UK Index Trust");
        s2.add(new Month(2, 2001), 129.6);
        s2.add(new Month(3, 2001), 123.2);
        s2.add(new Month(4, 2001), 117.2);
        s2.add(new Month(5, 2001), 124.1);
        s2.add(new Month(6, 2001), 122.6);
        s2.add(new Month(7, 2001), 119.2);
        s2.add(new Month(8, 2001), 116.5);
        s2.add(new Month(9, 2001), 112.7);
        s2.add(new Month(10, 2001), 101.5);
        s2.add(new Month(11, 2001), 106.1);
        s2.add(new Month(12, 2001), 110.3);
        s2.add(new Month(1, 2002), 111.7);
        s2.add(new Month(2, 2002), 111.0);
        s2.add(new Month(3, 2002), 109.6);
        s2.add(new Month(4, 2002), 113.2);
        s2.add(new Month(5, 2002), 111.6);
        s2.add(new Month(6, 2002), 108.8);
        s2.add(new Month(7, 2002), 101.6);

        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(s1);
        dataset.addSeries(s2);
        return dataset;
    }
}
