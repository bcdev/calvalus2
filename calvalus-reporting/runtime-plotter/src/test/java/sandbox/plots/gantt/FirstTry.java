package sandbox.plots.gantt;

import org.jfree.ui.RefineryUtilities;

import javax.swing.*;

public class FirstTry extends JFrame{

    public FirstTry(String title) {
        super(title);
        final JPanel chartPanel = new JPanel();
        setContentPane(chartPanel);
    }

//     public static JPanel createChartPanel() {
//        JFreeChart chart = ChartFactory.createGanttChart(
//            "Gantt Chart Demo",  // chart title
//            "Task",              // domain axis label
//            "Date",              // range axis label
//            dataset,             // data
//            true,                // include legend
//            true,                // tooltips
//            false                // urls
//        );;
//        return new ChartPanel(chart);
//    }

    public static void main(String[] argv) {
        JFrame f = new FirstTry("JFrame");

        JMenuBar menuBar = new JMenuBar();
        final JMenu jMenu = new JMenu("Datei");
        menuBar.add(jMenu).add(new JMenuItem("Anfangen"));
        jMenu.add(new JMenuItem("Beenden"));
        f.setJMenuBar(menuBar);

        f.setSize(400, 400);
        f.setLocation(100, 100);
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.setVisible(true);
        RefineryUtilities.centerFrameOnScreen(f);
    }


}
