/* --------------------
 * RegressionDemo1.java
 * --------------------
 * (C) Copyright 2002-2008, by Object Refinery Limited.
 *
 */

package sandbox.regressionPlots;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.axis.LogAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.DomainOrder;
import org.jfree.data.function.Function2D;
import org.jfree.data.function.LineFunction2D;
import org.jfree.data.general.DatasetChangeListener;
import org.jfree.data.general.DatasetGroup;
import org.jfree.data.general.DatasetUtilities;
import org.jfree.data.statistics.Regression;
import org.jfree.data.statistics.Statistics;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import sandbox.util.Utils;

import java.awt.*;

/**
 * A demo showing one way to fit regression lines to XY data.
 */
public class Example_RegressionPlotLogarithmic {
    private XYDataset dataSet;
    private Double[][] dataArray;
    private static Font font = new Font(Font.SANS_SERIF, Font.PLAIN, 17);

    public static void main(String args[]) {
        final Example_RegressionPlotLogarithmic regressionPlotLinear = new Example_RegressionPlotLogarithmic();
        regressionPlotLinear.dataSet = regressionPlotLinear.createSampleData1();
        Utils.saveChartOnScreen(600, 600, regressionPlotLinear.createChart());
    }

    private JFreeChart createChart() {

        NumberAxis xAxis = new NumberAxis("X");
        LogAxis yAxis = new LogAxis("Y");
         yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        final XYPlot plot = new XYPlot(null, xAxis, yAxis, null);

        //dataset 1
        plot.setDataset(0, dataSet);
        final boolean lines1 = false;
        final boolean shapes1 = true;
        XYLineAndShapeRenderer renderer1 = new XYLineAndShapeRenderer(lines1, shapes1);
//        XYSplineRenderer renderer1 = new XYSplineRenderer(5);
        renderer1.setSeriesPaint(0, Color.BLACK);
        plot.setRenderer(0, renderer1);

        //regression function calculation

//        double[] coefficients = Regression.getOLSRegression(new XLogYDataset(dataSet), 0);
        double[] coefficients = Regression.getOLSRegression(dataSet, 0);
        Function2D formula = new LineFunction2D(coefficients[0], coefficients[1]);

        //dataset2
        XYDataset regressionData = DatasetUtilities.sampleFunction2D(formula, 2.0, 11.0, 100, "Fitted Regression Line");
        plot.setDataset(1, regressionData);
        final boolean lines2 = true;
        final boolean shapes2 = false;
        XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer(lines2, shapes2);
        renderer2.setSeriesPaint(0, Color.BLACK);
        plot.setRenderer(1, renderer2);

        //statistics
        final double r = Statistics.getCorrelation(dataArray[0], dataArray[1]);
        final double delta = Statistics.getStdDev(dataArray[1]);
        final XYTextAnnotation annotationR = new XYTextAnnotation("r = " + r, 8.5, 55);
        final XYTextAnnotation annotationDelta = new XYTextAnnotation("delta = " + delta, 8, 50);
        annotationDelta.setFont(font);
        annotationR.setFont(font);
        plot.addAnnotation(annotationR);
        plot.addAnnotation(annotationDelta);

        return new JFreeChart("Linear Regression", JFreeChart.DEFAULT_TITLE_FONT, plot, true);
    }


    private XYDataset createSampleData1() {
        XYSeries series = new XYSeries("Series 1");
        dataArray = new Double[2][10];
        dataArray[0][0] = 2.0;
        dataArray[1][0] = 56.27;
        series.add(2.0, 56.27);
        dataArray[0][1] = 3.0;
        dataArray[1][1] = 41.32;
        series.add(3.0, 41.32);
        dataArray[0][2] = 4.0;
        dataArray[1][2] = 31.45;
        series.add(4.0, 31.45);
        dataArray[0][3] = 5.0;
        dataArray[1][3] = 30.05;
        series.add(5.0, 30.05);
        dataArray[0][4] = 6.0;
        dataArray[1][4] = 24.69;
        series.add(6.0, 24.69);
        dataArray[0][5] = 7.0;
        dataArray[1][5] = 19.78;
        series.add(7.0, 19.78);
        dataArray[0][6] = 8.0;
        dataArray[1][6] = 20.94;
        series.add(8.0, 20.94);
        dataArray[0][7] = 9.0;
        dataArray[1][7] = 16.73;
        series.add(9.0, 16.73);
        dataArray[0][8] = 10.0;
        dataArray[1][8] = 14.21;
        series.add(10.0, 14.21);
        dataArray[0][9] = 11.0;
        dataArray[1][9] = 12.44;
        series.add(11.0, 12.44);

        return new XYSeriesCollection(series);
    }

    private static class XLogYDataset implements XYDataset {

        private XYDataset dataset;

        private XLogYDataset(XYDataset dataset) {
            this.dataset = dataset;
        }

        @Override
        public DomainOrder getDomainOrder() {
            return dataset.getDomainOrder();
        }

        @Override
        public int getItemCount(int series) {
            return dataset.getItemCount(series);
        }

        @Override
        public Number getX(int series, int item) {
            return dataset.getX(series, item);
        }

        @Override
        public double getXValue(int series, int item) {
            return dataset.getXValue(series, item);
        }

        @Override
        public Number getY(int series, int item) {
            final Number y = dataset.getY(series, item);
            return Math.log10(y.doubleValue());
        }

        @Override
        public double getYValue(int series, int item) {
            final double value = dataset.getYValue(series, item);
            return Math.log10(value);
        }

        @Override
        public int getSeriesCount() {
            return dataset.getSeriesCount();
        }

        @Override
        public Comparable getSeriesKey(int series) {
            return dataset.getSeriesKey(series);
        }

        @Override
        public int indexOf(Comparable seriesKey) {
            return dataset.indexOf(seriesKey);
        }

        @Override
        public void addChangeListener(DatasetChangeListener listener) {
            dataset.addChangeListener(listener);
        }

        @Override
        public void removeChangeListener(DatasetChangeListener listener) {
            dataset.removeChangeListener(listener);
        }

        @Override
        public DatasetGroup getGroup() {
            return dataset.getGroup();
        }

        @Override
        public void setGroup(DatasetGroup group) {
            dataset.setGroup(group);
        }
    }
}
