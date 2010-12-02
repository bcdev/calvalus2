package sandbox.plots.gantt;

import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.io.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.lowagie.text.*;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.gantt.Task;
import org.jfree.data.gantt.TaskSeries;
import org.jfree.data.gantt.TaskSeriesCollection;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.util.Calendar;
import java.util.Date;

public class GanttChartSimple {
    private final static Logger LOGGER = Logger.getAnonymousLogger();
    private static String userHomeTemp;
    static {
      userHomeTemp = System.getProperty("user.home") + "/temp/";
    }

    public static void main(String[] args) throws IOException {

        //1 ) create Dataset
        TaskSeriesCollection taskSeriesCollection = createDataSet();

        //2) create JFreeChart object
        final JFreeChart chart = ChartFactory.createGanttChart(
                "My First Gantt Chart",  // title
                "Category Axis Label",   // domain axis label
                "Date",                  // range axis label
                taskSeriesCollection,    // data set
                true,                    // include legend
                true,                    // tooltips
                true                     // url
        );

        //3) draw the chart to some output
        saveChartAsPng(chart, 800, 400);
        LOGGER.info("ready png");
        saveChartAsJpg(chart, 800, 300);
        LOGGER.info("ready jpg");
        saveChartAsPDF(chart, 800, 200);
        LOGGER.info("ready pdf");
    }

    static TaskSeriesCollection createDataSet() {
        TaskSeriesCollection taskSeriesCollection = new TaskSeriesCollection();
        final TaskSeries taskSeries1 = createTaskSeries1();
        final TaskSeries taskSeries2 = createTaskSeries2();
        final TaskSeries taskSeries3 = createTaskSeries3();

        taskSeriesCollection.add(taskSeries1);
        taskSeriesCollection.add(taskSeries2);
        taskSeriesCollection.add(taskSeries3);
        return taskSeriesCollection;
    }


    private static void saveChartAsPDF(final JFreeChart chart, int width, int height) throws IOException {

        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(userHomeTemp + "GanttChartSimple.pdf")));
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
    }


    static void saveChartAsPng(JFreeChart chart, int width, int height) throws IOException {
        // http://www.jfree.org/phpBB2/viewtopic.php?t=1012
        // BufferedImage may need an XWindows system.
        // To workaround set property:
//        System.setProperty("java.awt.headless","true");
        final BufferedImage bufferedImage = chart.createBufferedImage(width, height);
        ImageIO.write(bufferedImage, "png", new File(userHomeTemp + "GanttChartSimple.png"));
    }

    private static void saveChartAsJpg(JFreeChart chart, int width, int height) throws IOException {
        final BufferedImage bufferedImage = chart.createBufferedImage(width, height);
//        chart.draw();
        bufferedImage.createGraphics();
//        ImageIO.write(bufferedImage, "jpg", new File(userHomeTemp + "GanttChartSimple.jpg"));
        ImageIO.write(bufferedImage, "jpg", new File(userHomeTemp + "GanttChartSimple.jpg"));
    }

    /*create jobs*/
    private static TaskSeries createTaskSeries1() {
        final TaskSeries taskSeries1 = new TaskSeries("job1");
        taskSeries1.add(new Task("task11", date(1, Calendar.JANUARY, 2009), date(1, Calendar.FEBRUARY, 2009)));
        taskSeries1.add(new Task("task25", date(11, Calendar.JANUARY, 2009), date(11, Calendar.FEBRUARY, 2009)));
        taskSeries1.add(new Task("task3", date(20, Calendar.MARCH, 2009), date(31, Calendar.MARCH, 2009)));

        final List listOfTask = taskSeries1.getTasks();
        return taskSeries1;
    }

    private static TaskSeries createTaskSeries2() {
        final TaskSeries taskSeries2 = new TaskSeries("job2");
        /*
         * Tasks with the same name across different task series are plotted inside the same category
         * (grouped along the y-axis/category axis)
         */
        taskSeries2.add(new Task("task3", date(1, 5, 2009), date(1, 6, 2009)));
        taskSeries2.add(new Task("task25", date(11, 5, 2009), date(11, 6, 2009)));
        taskSeries2.add(new Task("task11", date(20, 7, 2009), date(31, 7, 2009)));

        return taskSeries2;
    }

    private static TaskSeries createTaskSeries3() {
        final TaskSeries taskSeries3 = new TaskSeries("job3");
        final Task task1 = new Task("task6", date(1, 5, 2009), date(1, 6, 2009));
        task1.addSubtask(new Task("subTask1", date(1, 5, 2009), date(5, 5, 2009)));
        task1.addSubtask(new Task("subTask2", date(20, 5, 2009), date(25, 5, 2009)));
        task1.addSubtask(new Task("subTask3", date(26, 5, 2009), date(28, 5, 2009)));
        taskSeries3.add(task1);
        final Task task2 = new Task("task2", date(11, 5, 2009), date(11, 6, 2009));
        task2.addSubtask(new Task("task2", date(11, 5, 2009), date(15, 5, 2009)));
        task2.addSubtask(new Task("task2", date(22, 5, 2009), date(1, 6, 2009)));
        task2.addSubtask(new Task("task2", date(5, 6, 2009), date(11, 6, 2009)));
//        task2.setDescription("desc of task2");
        taskSeries3.add(task2);
        final Task task3 = new Task("task3", date(20, 7, 2009), date(31, 7, 2009));
        task3.addSubtask(new Task("task3", date(20, 7, 2009), date(22, 7, 2009)));
        task3.addSubtask(new Task("task3", date(26, 7, 2009), date(31, 7, 2009)));
        taskSeries3.add(task3);

        return taskSeries3;
    }


    private static Date date(int day, int month, int year) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, day);
        return calendar.getTime();

    }
}
