package sandbox.xyPlots;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.XYLineAnnotation;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.data.Range;
import org.jfree.data.xy.XYDataset;
import sandbox.util.Utils;

import java.awt.*;

public class Example_ScatterPlotCustomised {
    private static Font font = new Font(Font.SANS_SERIF, Font.PLAIN, 17);
    private static double xMax = 400.;
    private static double yMax = 400.;

    public static void main(String[] args) {
        JFreeChart chart = createChart(new Example_XYDatasetSimple());
        Utils.saveChartOnScreen(600, 600, chart);
//        Utils.saveChartAsPng(600, 600, chart, "myScatter");
    }

    private static JFreeChart createChart(XYDataset dataset) {
        boolean hasLegend = true;
        boolean tooltips = false;
        boolean url = false;
        final String title = "A Scatter Plot Title";
        final String labelX = "X (mg/L \u00b2 )";
        final String labelY = "Y (m h \u207b\u00b3)";
        JFreeChart chart = Example_ChartFactory.createScatterPlot(title, labelX, labelY, dataset,
                                                                  PlotOrientation.VERTICAL,
                                                                  hasLegend, tooltips, url);

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setNoDataMessage("NO DATA");

        /**Renderer**/
        final int rendererCount = plot.getRendererCount();
        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
        renderer.setSeriesOutlinePaint(2, Color.red); //symbols of points per series
        renderer.setUseOutlinePaint(false); //symbols of points per series
        renderer.setBaseCreateEntities(true);
//        renderer.setBaseShape(new Rectangle(4, 4));   //no effect!
        plot.setRenderer(0, renderer);


        final XYLineAndShapeRenderer renderer1 = new XYLineAndShapeRenderer(true, false);
        plot.setRenderer(1, renderer1);
        for (int i = 0; i < dataset.getSeriesCount(); i++) {
            renderer.setSeriesPaint(i, Color.BLACK); //symbols' fill colour
//            renderer.setSeriesFillPaint(i, Color.white); //effect
//            renderer.setSeriesOutlinePaint(i, Color.CYAN);  //effect
            renderer.setSeriesOutlineStroke(i, new BasicStroke(1)); //width of symbols' outlines
            renderer.setSeriesShapesFilled(i, false); //if symbol is filled
            renderer.setSeriesLinesVisible(2, false);  //
        }

        customiseAnnotation(plot);
        customiseAxis(plot);
        customiseLegend(chart);
        chart.setBackgroundPaint(new Color(255, 255, 255));
        plot.setDomainCrosshairVisible(true);
        plot.setRangeCrosshairVisible(true);
        return chart;
    }

    private static void customiseLegend(JFreeChart chart) {
        LegendTitle legend = chart.getLegend();
        if (legend != null) {
            legend.setItemFont(font);
        }

    }

    private static void customiseAnnotation(XYPlot plot) {
        final XYTextAnnotation annotation = new XYTextAnnotation("Text (180,180)", 180, 180);
        annotation.setFont(font);
        plot.addAnnotation(annotation);

        final XYLineAnnotation xyLineAnnotation = new XYLineAnnotation(0,0, xMax, yMax,
                                                                       new BasicStroke(0.02f), Color.green);
        plot.addAnnotation(xyLineAnnotation);

        plot.addDomainMarker(new ValueMarker(plot.getDataset().getYValue(1, 5)));  //todo no effect
    }

    private static void customiseAxis(XYPlot plot) {
        NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
        domainAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        domainAxis.setTickMarkInsideLength(6.0f);
        domainAxis.setTickMarkOutsideLength(-0.2f);
        domainAxis.setMinorTickCount(5);
        domainAxis.setMinorTickMarksVisible(false);
        domainAxis.setTickMarkStroke(new BasicStroke(2f)); //thickness of the tick marks
        domainAxis.setTickMarkPaint(Color.BLACK);
        domainAxis.setRange(new Range(0, yMax));
        domainAxis.setLabelFont(font);
        domainAxis.setTickLabelFont(font);

        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        rangeAxis.setTickMarkInsideLength(6.0f);
        rangeAxis.setTickMarkOutsideLength(-0.2f);
        rangeAxis.setMinorTickCount(2);
        rangeAxis.setMinorTickMarksVisible(false);
        rangeAxis.setTickMarkStroke(new BasicStroke(2f)); //thickness of the tick marks
        rangeAxis.setTickMarkPaint(Color.BLACK);
        rangeAxis.setRange(new Range(0, xMax));
        rangeAxis.setLabelFont(font);
        rangeAxis.setTickLabelFont(font);
    }

}