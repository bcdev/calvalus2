package sandbox.xyPlots;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.labels.XYToolTipGenerator;
import org.jfree.chart.plot.FastScatterPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.urls.StandardXYURLGenerator;
import org.jfree.chart.urls.XYURLGenerator;
import org.jfree.data.xy.XYDataset;

public class Example_ChartFactory {

    /**
     * The difference between scatter plot and xy line plot is the initialisation of the renderer:
     * new XYLineAndShapeRenderer(lines, shapes)
     * <p/>
     * scatter plot: new XYLineAndShapeRenderer(false, true)
     * xy line plot: new XYLineAndShapeRenderer(true, false);
     * *
     */
    public static JFreeChart createScatterPlot(String title, String xAxisLabel,
                                               String yAxisLabel, XYDataset dataset, PlotOrientation orientation,
                                               boolean legend, boolean tooltips, boolean urls) {

        if (orientation == null) {
            throw new IllegalArgumentException("Null 'orientation' argument.");
        }
        //axis
        NumberAxis xAxis = new NumberAxis(xAxisLabel);
//        xAxis.setAutoRangeIncludesZero(false);
        NumberAxis yAxis = new NumberAxis(yAxisLabel);
//        yAxis.setAutoRangeIncludesZero(false);

        //renderer
        XYToolTipGenerator toolTipGenerator = null;
        if (tooltips) {
            toolTipGenerator = new StandardXYToolTipGenerator();
        }

        XYURLGenerator urlGenerator = null;
        if (urls) {
            urlGenerator = new StandardXYURLGenerator();
        }
        boolean lines = false;
        boolean shapes = true;
        XYItemRenderer renderer = new XYLineAndShapeRenderer(lines, shapes);
//        renderer.setBaseToolTipGenerator(toolTipGenerator);
//        renderer.setURLGenerator(urlGenerator);

        //plot
        XYPlot plot = new XYPlot(dataset, xAxis, yAxis, renderer);
        plot.setOrientation(orientation);

        return new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, legend);
    }


    /**
     * The difference between scatter plot and xy line plot is the initialisation of the renderer:
     * new XYLineAndShapeRenderer(lines, shapes)
     * <p/>
     * scatter plot: new XYLineAndShapeRenderer(false, true)
     * xy line plot: new XYLineAndShapeRenderer(true, false);
     * *
     */
    public static JFreeChart createXYLineChart(String title, String xAxisLabel, String yAxisLabel, XYDataset dataset,
                                               PlotOrientation orientation, boolean legend, boolean tooltips,
                                               boolean urls) {

        if (orientation == null) {
            throw new IllegalArgumentException("Null 'orientation' argument.");
        }
        //axis
        NumberAxis xAxis = new NumberAxis(xAxisLabel);
//        xAxis.setAutoRangeIncludesZero(false);
        NumberAxis yAxis = new NumberAxis(yAxisLabel);
//        yAxis.setAutoRangeIncludesZero(false);

        //renderer - configured to plot lines
        boolean lines = true;
        boolean shapes = true;
        XYItemRenderer renderer = new XYLineAndShapeRenderer(lines, shapes);
        if (tooltips) {
            renderer.setBaseToolTipGenerator(new StandardXYToolTipGenerator());
        }
        if (urls) {
            renderer.setURLGenerator(new StandardXYURLGenerator());
        }

        //plot
        XYPlot plot = new XYPlot(dataset, xAxis, yAxis, renderer);
        plot.setOrientation(orientation);

        return new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, legend);
    }


    public static JFreeChart createFastScatterPlot(String title, String xAxisLabel, String yAxisLabel, boolean legend) {

        NumberAxis xAxis = new NumberAxis(xAxisLabel);
        xAxis.setAutoRangeIncludesZero(false);
        NumberAxis yAxis = new NumberAxis(yAxisLabel);
        yAxis.setAutoRangeIncludesZero(false);

        final float[][] data = new float[2][5];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 5; j++) {
                data[i][j] = i + j;
            }
        }
        float[] x = new float[]{1.0f, 2.0f, 5.0f, 6.0f};
        float[] y = new float[]{7.3f, 2.7f, 8.9f, 1.0f};
        float[][] data1 = new float[][]{x, y};

        FastScatterPlot plot = new FastScatterPlot(data1, xAxis, yAxis);
        return new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, legend);
    }

}
