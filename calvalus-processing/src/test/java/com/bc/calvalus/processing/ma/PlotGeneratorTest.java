package com.bc.calvalus.processing.ma;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.DefaultIntervalXYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.junit.Ignore;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * @author Norman
 */
@Ignore
public class PlotGeneratorTest {

    public static void main(String[] args) throws IOException {
        ApplicationFrame appFrame = new ApplicationFrame("JFreeChart: Regression Demo 1");
        DefaultIntervalXYDataset dataset = createDataset();

        JFreeChart chart = PlotGenerator.createScatterPlotChart("Bussole", "CHL", "CHL In-Situ", "CHL Satellite", dataset);

        ChartPanel chartPanel = new ChartPanel(chart, false);
        chartPanel.setPreferredSize(new Dimension(500, 500));

        appFrame.add(chartPanel);
        appFrame.pack();
        RefineryUtilities.centerFrameOnScreen(appFrame);
        appFrame.setVisible(true);

        writeChartAsImage(500, chart, 500, new File("scatterplot-"+System.currentTimeMillis()+".png"));
    }

    private static void writeChartAsImage(int width, JFreeChart chart, int height, File file) throws IOException {
        final BufferedImage bufferedImage = chart.createBufferedImage(width, height);
        ImageIO.write(bufferedImage, "PNG", file);
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
