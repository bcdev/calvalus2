package sandbox.boxWhiskerPlots;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYBoxAndWhiskerRenderer;
import org.jfree.data.statistics.BoxAndWhiskerCalculator;
import org.jfree.data.xy.AbstractXYDataset;
import sandbox.util.Utils;

import java.awt.*;

public class Example_BoxAndWhisker_XY {
    private static Font font = new Font(Font.SANS_SERIF, Font.PLAIN, 17);

    public static void main(String[] args) {
        JFreeChart chart = createCustomisedChart(createXYDataset());
        Utils.saveChartOnScreen(600, 600, chart);
    }

    private static JFreeChart createCustomisedChart(AbstractXYDataset dataset) {
        JFreeChart chart = Example_ChartFactory.createBoxAndWhiskerPlot(dataset);
        chart.setBackgroundPaint(Color.white);

        final XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.white);
        plot.setDomainGridlinesVisible(true);
        plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);

        final ValueAxis xAxis = plot.getRangeAxis();
        xAxis.setRange(((Example_BoxAndWhiskerXYDataset) dataset).getRangeLowerBound(true),
                       ((Example_BoxAndWhiskerXYDataset) dataset).getRangeUpperBound(true));
        xAxis.setLabelFont(font);
        xAxis.setTickLabelFont(font);
        xAxis.setTickMarkInsideLength(6.0f);
        xAxis.setTickMarkOutsideLength(-0.2f);
        xAxis.setMinorTickCount(5);
        xAxis.setMinorTickMarksVisible(false);
        xAxis.setTickMarkStroke(new BasicStroke(2f)); //thickness of the tick marks
        xAxis.setTickMarkPaint(Color.BLACK);


        final ValueAxis yAxis = plot.getDomainAxis();
        yAxis.setLabelFont(font);
        yAxis.setTickLabelFont(font);
        yAxis.setTickMarkInsideLength(6.0f);
        yAxis.setTickMarkOutsideLength(-0.2f);
        yAxis.setMinorTickCount(5);
        yAxis.setMinorTickMarksVisible(true);
        yAxis.setMinorTickMarkOutsideLength(-0.2f);
        yAxis.setTickMarkStroke(new BasicStroke(1.5f)); //thickness of the tick marks
        yAxis.setTickMarkPaint(Color.BLACK);


        final XYBoxAndWhiskerRenderer renderer = (XYBoxAndWhiskerRenderer) plot.getRenderer();
        renderer.setBoxWidth(15);
        renderer.setArtifactPaint(Color.BLACK); // colour of the data points
        renderer.setBaseItemLabelsVisible(true); //no effect
        renderer.setBoxPaint(Color.LIGHT_GRAY);
        renderer.setFillBox(true);
        renderer.setSeriesOutlineStroke(0, new BasicStroke(2)); //Boundary's width of a box
        renderer.setSeriesPaint(0, Color.black);
        renderer.setSeriesFillPaint(0, Color.magenta); //no effect
        renderer.setSeriesItemLabelsVisible(0, true); //no effect
        return chart;
    }

    private static AbstractXYDataset createXYDataset() {
        final int VALUE_COUNT = 20;
        //is a real XYDataset
        Example_BoxAndWhiskerXYDataset result = new Example_BoxAndWhiskerXYDataset("XY Dataset");

        for (int i = 0; i < 10; i++) {
            java.util.List values = Utils.createYValueList(0, 20.0, VALUE_COUNT);
            result.add(i + 1., BoxAndWhiskerCalculator.calculateBoxAndWhiskerStatistics(values));

        }
        return result;
    }
}
