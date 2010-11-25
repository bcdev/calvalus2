package com.bc.calvalus.plot;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
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

    public static JFreeChart createGanttChart(String title,
                                              String categoryAxisLabel,
                                              String dateAxisLabel,
                                              IntervalCategoryDataset dataset,
                                              boolean legend,
                                              boolean tooltips,
                                              boolean urls) {

        final GanttRenderer renderer = new GanttRenderer();
        renderer.setShadowVisible(false);

        if (tooltips) {
            renderer.setBaseToolTipGenerator(
                    new IntervalCategoryToolTipGenerator("{3} - {4}", DateFormat.getDateInstance()));
        }
        if (urls) {
            renderer.setBaseItemURLGenerator(new StandardCategoryURLGenerator());
        }

        CategoryPlot plot = new CategoryPlot(dataset, new CategoryAxis(categoryAxisLabel),
                                             new DateAxis(dateAxisLabel), renderer);

        plot.setOrientation(PlotOrientation.HORIZONTAL);
        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, legend);
//        new StandardChartTheme("JFree").apply(chart);
        return chart;
    }
}
