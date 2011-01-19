package sandbox.xyPlots;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.Range;
import org.jfree.data.xy.XYDataset;
import sandbox.util.Utils;

import java.awt.*;

public class Example_XYLinePlotSimple {

    public static void main(String[] args) {
        JFreeChart chart = createChart(new Example_XYDatasetContinuous(30));
        Utils.saveChartOnScreen(600, 600, chart);
//        Utils.saveChartAsPng(600, 600, chart, "myXYLinePlot");
    }

    private static JFreeChart createChart(XYDataset dataset) {
        boolean legend = true;
        boolean tooltips = true;
        boolean url = true;
        final String title = "A XYLineChart";
        final String labelX = "X";
        final String labelY = "Y";
        JFreeChart chart = Example_ChartFactory.createXYLineChart(title, labelX, labelY, dataset,
                                                                  PlotOrientation.VERTICAL,
                                                                  legend, tooltips, url);

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setNoDataMessage("NO DATA");
        plot.setDomainZeroBaselineVisible(false);
        plot.setRangeZeroBaselineVisible(false);
        final XYTextAnnotation annotation = new XYTextAnnotation("Annotation (100,100)", 100, 100);
        annotation.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        plot.addAnnotation(annotation);

        customiseAxisToEqualScales(plot, dataset);

        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
        renderer.setSeriesOutlinePaint(2, Color.red); //symbols of points per series
        renderer.setUseOutlinePaint(false); //symbols of points per series
        renderer.setBaseCreateEntities(true);

        chart.setBackgroundPaint(new Color(255, 255, 255));
        return chart;
    }

    private static void customiseAxisToEqualScales(XYPlot plot, XYDataset dataset) {
        //find range for quadratic plot
        double max = 0;
        double min = 0;
        if (dataset instanceof Example_XYDatasetContinuous) {
            final Range rangeRange = ((Example_XYDatasetContinuous) dataset).getRangeRange();
            final Range domainRange = ((Example_XYDatasetContinuous) dataset).getDomainRange();
            max = Math.max(rangeRange.getUpperBound(), domainRange.getUpperBound());
            min = Math.min(rangeRange.getLowerBound(), domainRange.getLowerBound());
        }

        //configure x axis
        NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
        domainAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        domainAxis.setTickMarkInsideLength(5.0f);
        domainAxis.setTickMarkOutsideLength(0.0f);
        domainAxis.setMinorTickMarksVisible(false);
        if (dataset instanceof Example_XYDatasetContinuous) {
            domainAxis.setRange(min, max);
        }

        //configure y axis
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        rangeAxis.setTickMarkInsideLength(5.0f);
        rangeAxis.setTickMarkOutsideLength(0.0f);
        rangeAxis.setMinorTickCount(2);
        rangeAxis.setMinorTickMarksVisible(false);
        rangeAxis.setRange(new Range(min, max));
    }
}