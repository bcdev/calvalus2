package com.bc.calvalus.plot;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.labels.IntervalCategoryToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.GanttRenderer;
import org.jfree.chart.urls.StandardCategoryURLGenerator;
import org.jfree.data.category.IntervalCategoryDataset;

import java.text.DateFormat;

/**
 * @see org.jfree.chart.ChartFactory
 */
public class ChartFactory {
    /**
     * The chart theme.

    private static ChartTheme currentTheme = new StandardChartTheme("JFree");
    */

    public static JFreeChart createGanttChart(String title,
                                              String categoryAxisLabel,
                                              String dateAxisLabel,
                                              IntervalCategoryDataset dataset,
                                              boolean legend,
                                              boolean tooltips,
                                              boolean urls) {

        CategoryAxis categoryAxis = new CategoryAxis(categoryAxisLabel);
        DateAxis dateAxis = new DateAxis(dateAxisLabel);

        GanttRenderer renderer = new GanttRenderer();
        renderer.setShadowVisible(false);

        if (tooltips) {
            renderer.setBaseToolTipGenerator(new IntervalCategoryToolTipGenerator(
                    "{3} - {4}", DateFormat.getDateInstance()));
        }
        if (urls) {
            renderer.setBaseItemURLGenerator(new StandardCategoryURLGenerator());
        }

        CategoryPlot plot = new CategoryPlot(dataset, categoryAxis, dateAxis, renderer);
        plot.setOrientation(PlotOrientation.HORIZONTAL);
        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, legend);
        //currentTheme.apply(chart);
        return chart;
    }
}
