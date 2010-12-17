package sandbox.xyPlots;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.Range;
import org.jfree.data.xy.XYDataset;
import sandbox.util.Utils;

import java.awt.*;

public class Example_ScatterPlotSimple {

    public static void main(String[] args) {
        JFreeChart chart = createChart(new Example_XYDatasetSimple());
        Utils.saveChartOnScreen(600, 600, chart);
//        Utils.saveChartAsPng(600, 600, chart, "myScatter");
    }

    private static JFreeChart createChart(XYDataset dataset) {
        boolean legend = true;
        boolean tooltips = true;
        boolean url = true;
        JFreeChart chart = Example_ChartFactory.createScatterPlot("A Scatter Plot", "X", "Y", dataset,
                                                                  PlotOrientation.VERTICAL,
                                                                  legend, tooltips, url);

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setNoDataMessage("NO DATA");
        plot.setDomainZeroBaselineVisible(false);
        plot.setRangeZeroBaselineVisible(false);
        final XYTextAnnotation annotation = new XYTextAnnotation("Annotation (100,100)", 100, 100);
        annotation.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        plot.addAnnotation(annotation);
        plot.addDomainMarker(new ValueMarker(plot.getDataset().getYValue(1, 5)));  // no effect

//        plot.setDomainTickBandPaint(new Color(0, 255, 255, 128));
//        plot.setDomainCrosshairVisible(true);
//        plot.setRangeCrosshairVisible(true);
//
//        plot.setDomainGridlineStroke(new BasicStroke(0.0f));
//        plot.setDomainMinorGridlineStroke(new BasicStroke(2.0f));
//        plot.setDomainGridlinePaint(Color.blue);
//        plot.setRangeGridlineStroke(new BasicStroke(0.0f));
//        plot.setRangeMinorGridlineStroke(new BasicStroke(2.0f));
//        plot.setRangeGridlinePaint(Color.blue);

//        plot.setDomainMinorGridlinesVisible(true);
//        plot.setRangeMinorGridlinesVisible(true);

        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
        renderer.setSeriesOutlinePaint(2, Color.red); //symbols of points per series
        renderer.setUseOutlinePaint(false); //symbols of points per series
        renderer.setBaseCreateEntities(true);
        renderer.setBaseShape(new Rectangle(4, 4));   //no effect!

        NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
        domainAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        domainAxis.setTickMarkInsideLength(5.0f);
        domainAxis.setTickMarkOutsideLength(0.0f);
//        domainAxis.setMinorTickCount(5);
        domainAxis.setMinorTickMarksVisible(false);
        domainAxis.setRange(new Range(0, 200));  //fix range

        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        rangeAxis.setTickMarkInsideLength(5.0f);
        rangeAxis.setTickMarkOutsideLength(0.0f);
        rangeAxis.setMinorTickCount(2);
        rangeAxis.setMinorTickMarksVisible(false);
        rangeAxis.setRange(new Range(0, 200));  //fix range


        chart.setBackgroundPaint(new Color(255, 255, 255));
        return chart;
    }
}