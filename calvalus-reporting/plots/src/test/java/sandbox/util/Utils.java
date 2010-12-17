package sandbox.util;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

public final class Utils {
    private Utils() {
    }

    /**
     * screen*
     */
    public static void saveChartOnScreen(int width, int height, JFreeChart chart) {
        final JFrame frame = new JFrame("JFrame Title");
        frame.setBounds(new Rectangle(width, height));
        final JPanel panel = createDemoPanel(chart);
        panel.setPreferredSize(new Dimension(width, height));
        frame.add(panel);
        frame.pack();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }

    /**
     * screen*
     */
    private static JPanel createDemoPanel(JFreeChart chart) {
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setVerticalAxisTrace(true);
        chartPanel.setHorizontalAxisTrace(true);
        chartPanel.setPopupMenu(null);
        chartPanel.setDomainZoomable(true);
        chartPanel.setRangeZoomable(true);
        return chartPanel;
    }

    public static void saveChartAsPng(int width, int height, JFreeChart chart, String fileName) {
        final BufferedImage bufferedImage = chart.createBufferedImage(width, height);
        try {
            ImageIO.write(bufferedImage, "png",
                          new File(System.getProperty("user.home") + "/temp/calvalus/" + fileName + ".png"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

     public static java.util.List createYValueList(double lowerBound, double upperBound, int count) {
        java.util.List result = new java.util.ArrayList();
        for (int i = 0; i < count; i++) {
            double v = lowerBound + (Math.random() * (upperBound - lowerBound));
            result.add(new Double(v));
        }
        return result;
    }
}
