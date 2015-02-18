package com.bc.calvalus.processing.ma;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.junit.Ignore;

import javax.imageio.ImageIO;
import java.awt.Dimension;
import java.io.File;
import java.io.IOException;

/**
 * @author Norman
 */
@Ignore
public class PlotGeneratorTest {

    public static void main(String[] args) throws IOException {

        PlotDatasetCollector collector = new PlotDatasetCollector("SITE");
        collector.processHeaderRecord(new Object[]{"SITE", "CHL", "chl"}, new Object[]{""});
        int n = 6;
        double delta = 1.0 / n;
        for (int i = 0; i < n; i++) {
            double ref = delta * (i + Math.random());
            double mean = delta * (i + Math.random());
            double sigma = 0.01 + 0.1 * Math.random();
            collector.processDataRecord("key1", new Object[]{"Boussole", ref, new AggregatedNumber(20, 25, 2, 0.0, 1.0, mean, sigma)}, new Object[]{""});
        }

        PlotDatasetCollector.PlotDataset[] plotDatasets = collector.getPlotDatasets();


        ApplicationFrame appFrame = new ApplicationFrame("JFreeChart: Regression Demo 1");

        PlotDatasetCollector.PlotDataset plotDataset = plotDatasets[0];
        final String title = plotDataset.getVariablePair().referenceAttributeName + " / " + plotDataset.getGroupName();

        JFreeChart chart = PlotGenerator.createChart(title, "beam-idepix 2.3, idepix.ComputeChain", "myinsitu.csv", plotDataset);

        ChartPanel chartPanel = new ChartPanel(chart, false);
        chartPanel.setPreferredSize(new Dimension(500, 500));

        appFrame.add(chartPanel);
        appFrame.pack();
        RefineryUtilities.centerFrameOnScreen(appFrame);
        appFrame.setVisible(true);

        ImageIO.write(chart.createBufferedImage(500, 500),
                      "PNG",
                      new File("scatterplot-" + System.currentTimeMillis() + ".png"));
    }


}
