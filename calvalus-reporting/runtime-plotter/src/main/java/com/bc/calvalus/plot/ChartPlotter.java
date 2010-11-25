package com.bc.calvalus.plot;

import com.lowagie.text.Document;
import com.lowagie.text.DocumentException;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.DefaultFontMapper;
import com.lowagie.text.pdf.PdfContentByte;
import com.lowagie.text.pdf.PdfTemplate;
import com.lowagie.text.pdf.PdfWriter;
//import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ChartPlotter {
    private final static Logger LOGGER = Logger.getAnonymousLogger();
    private static String userHomeTemp;
    private PlotterConfigurator plotterConfigurator;
    private JFreeChart chart;

    public ChartPlotter(PlotterConfigurator plotterConfigurator) {
        super();
        this.plotterConfigurator = plotterConfigurator;
    }

    static {
        userHomeTemp = System.getProperty("user.home") + "/temp/calvalus/";
    }

    public static void main(String[] args) throws IOException {
        // 1) configure
        if (args.length < 2 || !args[0].startsWith("-category=") || !args[1].startsWith("-colour=")) {
            System.out.println("Please enter a command like: java ChartPlotter -category=task -colour=host");
            System.out.println("Or enter a command like: java ChartPlotter -category=task -colour=job");
            System.exit(0);
        }
        final ChartPlotter chartPlotter = new ChartPlotter(new PlotterConfigurator());
        chartPlotter.getPlotterConfigurator().setCategory(args[0].split("=")[1]);
        chartPlotter.getPlotterConfigurator().setColouredDimension(args[1].split("=")[1]);

        // 2) execute
        chartPlotter.execute();
    }

    private void execute() throws IOException {
        //1) create Dataset
        final DataSetConverter dataSetConverter = new DataSetConverter();
        DataSetConverter.Filter dataFilter = dataSetConverter.createDataFilter(getPlotterConfigurator());

        //2) create JFreeChart object
        createChart(dataSetConverter, dataFilter);

        //3) some customisation
        customiseChart();

        //4) draw the chart to some output
        LOGGER.info("saving ...");
        saveChartAsPng(1500, 800);
        saveChartAsJpg(800, 300);
        saveChartAsPDF(1500, 800);
        saveChartOnScreen();
    }

    private void createChart(DataSetConverter dataSetConverter, DataSetConverter.Filter dataFilter) {
        LOGGER.info("creating gantt chart ...");
        chart = ChartFactory.createGanttChart(
                "Gantt Chart", "Category", "Date", dataSetConverter.createDataSet(dataFilter), true, true, true);
    }

    private void customiseChart() {
        LOGGER.info("customising ...");
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
    }

    private void saveChartOnScreen() {
        final ChartPanel chartPanel = new ChartPanel(chart);
        final JFrame frame = new JFrame("Gantt Test");
        frame.add(chartPanel);
        frame.pack();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
        LOGGER.info("is on screen");
    }


    private void saveChartAsPDF(int width, int height) throws IOException {

        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(userHomeTemp + "myImage.pdf")));
        Document document = new Document(new Rectangle(width, height));
        try {
            PdfWriter pdfWriter = PdfWriter.getInstance(document, out);
            document.open();
            PdfContentByte pdfContentByte = pdfWriter.getDirectContent();
            PdfTemplate pdfTemplate = pdfContentByte.createTemplate(width, height);

            Graphics2D graphics2D = pdfTemplate.createGraphics(width, height, new DefaultFontMapper());
            Rectangle2D rectangle2D = new Rectangle2D.Double(0, 0, width, height);
            chart.draw(graphics2D, rectangle2D);
            graphics2D.dispose();

            pdfContentByte.addTemplate(pdfTemplate, 0, 0);
        } catch (DocumentException de) {
            LOGGER.log(Level.SEVERE, "error while creating pdf", de);
        } finally {
            document.close();
            out.close();
        }
        LOGGER.info("ready pdf");
    }


    private void saveChartAsPng(int width, int height) throws IOException {
        // http://www.jfree.org/phpBB2/viewtopic.php?t=1012
        // BufferedImage may need an XWindows system.
        // To workaround set property:
//        System.setProperty("java.awt.headless","true");
        final BufferedImage bufferedImage = chart.createBufferedImage(width, height);
        ImageIO.write(bufferedImage, "png", new File(userHomeTemp + "myImage.png"));
        LOGGER.info("ready png");
    }

    private void saveChartAsJpg(int width, int height) throws IOException {
        final BufferedImage bufferedImage = chart.createBufferedImage(width, height);
        bufferedImage.createGraphics();
        ImageIO.write(bufferedImage, "jpg", new File(userHomeTemp + "myImage.jpg"));
        LOGGER.info("ready jpg");
    }

    public PlotterConfigurator getPlotterConfigurator() {
        return plotterConfigurator;
    }
}
