package com.bc.calvalus.processing.ma;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.DefaultIntervalXYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.junit.Ignore;

import javax.imageio.ImageIO;
import java.awt.*;
import java.io.File;
import java.io.IOException;

/**
 * @author Norman
 */
@Ignore
public class PlotGeneratorTest {

    public static void main(String[] args) throws IOException {

        PlotDatasetCollector collector = new PlotDatasetCollector("SITE");
        collector.processHeaderRecord(new Object[]{"SITE", "CHL", "chl"});
        int n = 6;
        double delta = 1.0 / n;
        for (int i = 0; i < n; i++) {
            double ref = delta * (i + Math.random());
            double mean = delta * (i + Math.random());
            double sigma = 0.01 + 0.1 * Math.random();
            collector.processDataRecord(i, new Object[]{"Boussole", ref, new AggregatedNumber(20, 25, 2, 0.0, 1.0, mean, sigma)});
        }

        PlotDatasetCollector.PlotDataset[] plotDatasets = collector.getPlotDatasets();


        ApplicationFrame appFrame = new ApplicationFrame("JFreeChart: Regression Demo 1");

        PlotDatasetCollector.PlotDataset plotDataset = plotDatasets[0];
        final String title = plotDataset.getVariablePair().referenceAttributeName + " / " + plotDataset.getGroupName();

        JFreeChart chart = PlotGenerator.createScatterPlotChart(title, "beam-idepix 2.3, idepix.ComputeChain", "myinsitu.csv", plotDataset);

        ChartPanel chartPanel = new ChartPanel(chart, false);
        chartPanel.setPreferredSize(new Dimension(500, 500));

        appFrame.add(chartPanel);
        appFrame.pack();
        RefineryUtilities.centerFrameOnScreen(appFrame);
        appFrame.setVisible(true);

        ImageIO.write(chart.createBufferedImage(500, 500),
                      "PNG",
                      new File("scatterplot-"+System.currentTimeMillis()+".png"));
    }

    private static DefaultIntervalXYDataset createDataset() {
        DefaultIntervalXYDataset dataset = new DefaultIntervalXYDataset();
        int n = 6;
        double[][] data = new double[6][n];
        double[] xData = data[0];
        double[] xsData = data[1];
        double[] xeData = data[2];
        double[] yData = data[3];
        double[] ysData = data[4];
        double[] yeData = data[5];
        double delta = 1.0 / n;
        for (int i = 0; i < n; i++) {
            xData[i] = delta * (i + Math.random());
            yData[i] = delta * (i + 2 * Math.random());
            xsData[i] = xeData[i] = xData[i];
            double dy = 0.1 * Math.random();
            ysData[i] = yData[i] - dy;
            yeData[i] = yData[i] + dy;
        }
        dataset.addSeries("Test series", data);
        return dataset;
    }


}
