package sandbox.plots.gantt;

import org.jfree.chart.ChartColor;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.data.gantt.TaskSeriesCollection;

import java.awt.*;
import java.io.IOException;
import java.util.logging.Logger;

public class GanttChartCustomisePlotAppearance {
    public static final Logger LOGGER = Logger.getAnonymousLogger();

    public static void main(String[] args) throws IOException {

        //1 ) create Dataset
        TaskSeriesCollection taskSeriesCollection = GanttChartSimple.createDataSet();

        //2) create JFreeChart object
        final JFreeChart chart = ChartFactory.createGanttChart(
                "My First Gantt Chart",  // title
                "Category Axis Label",   // domain axis label
                "Date",                  // range axis label
                taskSeriesCollection,    // data set
                true,                    // include legend
                true,                    // tooltips
                true                     // url
        );

        //3) do the customising
        chart.setAntiAlias(false);
        chart.setBorderVisible(false);
        chart.getTitle().setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 10));
        chart.setRenderingHints(new RenderingHints(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_SPEED));
        chart.setBackgroundPaint(new ChartColor(255, 255, 255));

        final CategoryAxis categoryAxis = chart.getCategoryPlot().getDomainAxis();
        categoryAxis.setLabelFont(new Font(Font.SANS_SERIF, Font.PLAIN, 10));
        categoryAxis.setTickLabelFont(new Font(Font.SANS_SERIF, Font.PLAIN, 8));

        final ValueAxis valueAxis = chart.getCategoryPlot().getRangeAxis();
        valueAxis.setLabelFont(new Font(Font.SANS_SERIF, Font.PLAIN, 10));
        valueAxis.setTickLabelFont(new Font(Font.SANS_SERIF, Font.PLAIN, 8));
        valueAxis.setTickLabelsVisible(true);

        //4) draw the chart to some output
        GanttChartSimple.saveChartAsPng(chart, 800, 400);
        LOGGER.info("ready png");
    }
}
