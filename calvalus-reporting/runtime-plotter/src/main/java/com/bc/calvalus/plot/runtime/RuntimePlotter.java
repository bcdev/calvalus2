package com.bc.calvalus.plot.runtime;

import com.bc.calvalus.plot.RunTimesScanner;
import com.bc.calvalus.plot.TimeUtils;
import com.bc.calvalus.plot.Trace;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.GanttRenderer;
import org.jfree.data.gantt.Task;
import org.jfree.data.gantt.TaskSeries;
import org.jfree.data.gantt.TaskSeriesCollection;

import javax.swing.JFrame;
import java.awt.Font;
import java.awt.RenderingHints;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * TODO add API doc
 *
 * @author
 */
public class RuntimePlotter {

    public static void main(String[] args) {

        try {
            // parse command line arguments
            final Args options = new Args(args);
            final String inputLog = options.getArgs()[0];
            final String category = options.get("category", "host");
            final String colour = options.get("colour", "job");
            long start = TimeUtils.parseCcsdsUtcFormat(options.get("start", ""));
            long stop = TimeUtils.parseCcsdsUtcFormat(options.get("stop", ""));

            // scan log file
            RunTimesScanner scanner = new RunTimesScanner(new BufferedReader(new FileReader(inputLog)));
            final List<Trace> traces = scanner.scan();
            scanner.getValids().add(category, "null");
            scanner.getValids().add(colour, "null");
            final List<String> categoryValids = new ArrayList<String>(scanner.getValids().get(category));
            final List<String> colourValids = new ArrayList<String>(scanner.getValids().get(colour));
            final long logStart = TimeUtils.parseCcsdsLocalTimeWithoutT(scanner.getStart());
            final long logStop = TimeUtils.parseCcsdsLocalTimeWithoutT(scanner.getStop());

            if (logStart != TimeUtils.TIME_NULL && (start == TimeUtils.TIME_NULL || start < logStart)) start = logStart;
            if (logStop != TimeUtils.TIME_NULL && (stop == TimeUtils.TIME_NULL || stop > logStop)) stop = logStop;

            // convert to JFreeChart task series
            final TaskSeriesCollection seriesCollection = new TaskSeriesCollection();
            for (String colourValue : colourValids) {
                seriesCollection.add(new TaskSeries(colourValue));
            }
            for (Trace trace : traces) {
                final String categoryValue = String.valueOf(trace.getPropertyValue(category));
                final String colourValue = String.valueOf(trace.getPropertyValue(colour));
                final int categoryIndex = categoryValids.indexOf(categoryValue);
                final int colourIndex = colourValids.indexOf(colourValue);
                // final int index = 25 * categoryIndex + trace.getId().hashCode() % 20; // TODO improve distribution of neighbours
                int p1 = trace.getId().indexOf('_');
                int p2 = trace.getId().indexOf('_', p1+1);
                int p3 = trace.getId().indexOf('_', p2+1);
                int p4 = trace.getId().indexOf('_', p3+1);
                int i1 = p1 != -1 ? p2 != -1 ? Integer.parseInt(trace.getId().substring(p1+1, p2)) : Integer.parseInt(trace.getId().substring(p1+1)) : 0;
                int i3 = p4 != -1 ? Integer.parseInt(trace.getId().substring(p3+1, p4)) : 0;
                int i4 = p4 != -1 ? Integer.parseInt(trace.getId().substring(p4+1)) : 0;
                int traceId = i1 + i3 + i4 * 10 / 2;              
                final int index = 20 * categoryIndex + traceId % 10;
                final TaskSeries series =  seriesCollection.getSeries(colourValue);
                // preliminarily exclude open traces
                //if (trace.getStartTime() == TimeUtils.TIME_NULL || trace.getStopTime() == TimeUtils.TIME_NULL) continue;
                // check for interval
                if (trace.getStopTime() != TimeUtils.TIME_NULL && trace.getStopTime() < start) continue;
                if (trace.getStartTime() != TimeUtils.TIME_NULL && trace.getStartTime() > stop) continue;
                long traceStart = trace.getStartTime();
                if (traceStart == TimeUtils.TIME_NULL || traceStart < start) traceStart = start;
                long traceStop = trace.getStopTime();
                if (traceStop == TimeUtils.TIME_NULL || traceStop > stop) traceStop = stop;
                //
                series.add(new Task(String.valueOf(index), new Date(traceStart), new Date(traceStop)));
                System.out.println(trace);
            }

            // construct chart
            final GanttRenderer renderer = new GanttRenderer();
            renderer.setShadowVisible(false);
            //renderer.setBaseToolTipGenerator(new IntervalCategoryToolTipGenerator("{3} - {4}", DateFormat.getDateInstance()));
            //renderer.setBaseItemURLGenerator(new StandardCategoryURLGenerator());
            CategoryPlot plot = new CategoryPlot(seriesCollection,
                                                 new CategoryAxis(category),
                                                 new DateAxis("start and stop time"),
                                                 renderer);
            plot.setOrientation(PlotOrientation.HORIZONTAL);
            JFreeChart chart = new JFreeChart("Runtimes per " + category + " and " + colour,
                                              JFreeChart.DEFAULT_TITLE_FONT,
                                              plot,
                                              true);
            final Font defaultFont = new Font(Font.SANS_SERIF, Font.PLAIN, 10);
            final Font titleFont = new Font(Font.SANS_SERIF, Font.PLAIN, 15);
            final Font tickLabelFont = new Font(Font.SANS_SERIF, Font.PLAIN, 8);
            chart.setAntiAlias(true);
            chart.setBorderVisible(false);
            chart.getTitle().setFont(titleFont);
            chart.setRenderingHints(new RenderingHints(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_SPEED));
            final CategoryPlot categoryPlot = chart.getCategoryPlot();
            final CategoryAxis categoryAxis = chart.getCategoryPlot().getDomainAxis();
            categoryAxis.setLabelFont(defaultFont);
            categoryAxis.setTickLabelsVisible(false);
            final ValueAxis valueAxis = chart.getCategoryPlot().getRangeAxis();
            valueAxis.setLabelFont(defaultFont);
            valueAxis.setTickLabelFont(tickLabelFont);
            chart.getLegend().setItemFont(defaultFont);

            // paint to frame
            final ChartPanel chartPanel = new ChartPanel(chart);
            final JFrame frame = new JFrame("Gantt Test");
            frame.add(chartPanel);
            frame.pack();
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setVisible(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
