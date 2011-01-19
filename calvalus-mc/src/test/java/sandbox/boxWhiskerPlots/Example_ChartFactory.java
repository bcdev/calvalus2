package sandbox.boxWhiskerPlots;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.LogAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYBoxAndWhiskerRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.statistics.DefaultBoxAndWhiskerXYDataset;
import org.jfree.data.xy.AbstractXYDataset;

public class Example_ChartFactory {

    public static JFreeChart createBoxAndWhiskerPlot(AbstractXYDataset dataset) {
        String title = "Box-and-Whisker Chart";
        String xAxisLabel = "X (m h \u207b\u2074)";
        String yAxisLabel = "Value (m h\u207b1)";
        String xAxisDateLabel = "Day (month)";

        XYItemRenderer renderer = new XYBoxAndWhiskerRenderer();

        LogAxis rangeAxis = new LogAxis(yAxisLabel); //logarithmic axis
//        NumberAxis rangeAxis = new NumberAxis(yAxisLabel);
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        XYPlot plot;
        if (dataset instanceof DefaultBoxAndWhiskerXYDataset) {
            plot = new XYPlot(dataset, new DateAxis(xAxisDateLabel), rangeAxis, renderer);
        } else if (dataset instanceof Example_BoxAndWhiskerXYDataset) {
            plot = new XYPlot(dataset, new NumberAxis(xAxisLabel), rangeAxis, renderer);
        } else {
            throw new RuntimeException("DefaultBoxAndWhiskerXYDataset or Example_BoxAndWhiskerXYDataset required.");
        }
        plot.setNoDataMessage("NO DATA");
        return new JFreeChart(title, plot);
    }
}
