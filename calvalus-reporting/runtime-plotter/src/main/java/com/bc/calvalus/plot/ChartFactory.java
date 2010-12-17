package com.bc.calvalus.plot;

import com.bc.calvalus.plot.utility.CombinedCategoryPlot;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.labels.IntervalCategoryToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.CombinedDomainCategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.GanttRenderer;
import org.jfree.chart.urls.StandardCategoryURLGenerator;
import org.jfree.data.category.IntervalCategoryDataset;

import java.text.DateFormat;
import java.util.List;

/**
 * @see org.jfree.chart.ChartFactory
 */
public class ChartFactory {


    public static JFreeChart createGanttChart(String title, String categoryAxisLabel, String dateAxisLabel,
                                              IntervalCategoryDataset dataset,
                                              boolean legend, boolean tooltips, boolean urls) {

        final GanttRenderer renderer = new GanttRenderer();
        renderer.setShadowVisible(false);

        if (tooltips) { //todo necessary?
            renderer.setBaseToolTipGenerator(
                    new IntervalCategoryToolTipGenerator("{3} - {4}", DateFormat.getDateInstance()));
        }
        if (urls) { //todo necessary?
            renderer.setBaseItemURLGenerator(new StandardCategoryURLGenerator());
        }

        CategoryPlot plot = new CategoryPlot(dataset, new CategoryAxis(categoryAxisLabel),
                                             new DateAxis(dateAxisLabel), renderer);

        plot.setOrientation(PlotOrientation.HORIZONTAL);
        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, legend);
//        new StandardChartTheme("JFree").apply(chart);
        return chart;
    }

    public static JFreeChart createGanttChartSubplots(String title, String categoryAxisLabel, String dateAxisLabel,
                                                      List<IntervalCategoryDataset> datasets,
                                                      boolean legend, boolean tooltips, boolean urls) {

        final GanttRenderer renderer = new GanttRenderer();
        renderer.setShadowVisible(false);

        final IntervalCategoryDataset dataset1 = datasets.get(0);
        final CategoryPlot plot1 = new CategoryPlot(dataset1, new CategoryAxis(categoryAxisLabel),
                                                    new DateAxis(dateAxisLabel), renderer);
        plot1.setOrientation(PlotOrientation.HORIZONTAL);
        final CategoryPlot plot2 = new CategoryPlot(datasets.get(1), new CategoryAxis(categoryAxisLabel),
                                                    new DateAxis(dateAxisLabel), renderer);
        plot2.setOrientation(PlotOrientation.HORIZONTAL);

        final CombinedDomainCategoryPlot parentPlot = new CombinedDomainCategoryPlot();
        parentPlot.add(plot1);
        parentPlot.add(plot2);


        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, parentPlot, legend);
        return chart;
    }


  public static JFreeChart createGanttChartSubplots_own(String title, String categoryAxisLabel, String dateAxisLabel,
                                                      List<IntervalCategoryDataset> datasets,
                                                      boolean legend, boolean tooltips, boolean urls) {


        final GanttRenderer renderer = new GanttRenderer();
        renderer.setShadowVisible(false);

        final IntervalCategoryDataset dataset1 = datasets.get(0);
        final CategoryPlot plot1 = new CategoryPlot(dataset1, new CategoryAxis(categoryAxisLabel),
                                                    new DateAxis(dateAxisLabel), renderer);
        plot1.setOrientation(PlotOrientation.HORIZONTAL);
        final CategoryPlot plot2 = new CategoryPlot(datasets.get(1), new CategoryAxis(categoryAxisLabel),
                                                    new DateAxis(dateAxisLabel), renderer);
        plot2.setOrientation(PlotOrientation.HORIZONTAL);

        final CombinedCategoryPlot parentPlot = new CombinedCategoryPlot();
        parentPlot.add(plot1);
        parentPlot.add(plot2);


        JFreeChart chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, parentPlot, legend);
        return chart;
    }

}
