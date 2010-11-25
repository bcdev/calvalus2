package com.bc.calvalus.plot;

import org.jfree.data.category.IntervalCategoryDataset;
import org.jfree.data.gantt.Task;
import org.jfree.data.gantt.TaskSeries;
import org.jfree.data.gantt.TaskSeriesCollection;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataSetConverter {
    private final static Logger LOGGER = Logger.getAnonymousLogger();

    private TaskSeriesCollection taskSeriesCollection;

    public DataSetConverter() {
        super();
        taskSeriesCollection = new TaskSeriesCollection();
    }

    private List<Trace> scanLogFiles() {
        RunTimesScanner runTimesScanner;
        final String fileName = PlotterConfigurator.getInstance().getInputFile();
        try {
            runTimesScanner = new RunTimesScanner(new BufferedReader(new FileReader(fileName)));
            return runTimesScanner.scan();
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "could not find file " + fileName, e);
            return null;
        }
    }

    public IntervalCategoryDataset createDataSet(Filter dataFilter) {
        final Map<String, TaskSeries> jobsMap = dataFilter.filter(scanLogFiles());
        final PlotterConfigurator plotterConfigurator = PlotterConfigurator.getInstance();
        plotterConfigurator.askForNumberOfSeriesToBeShown();
        
        for (TaskSeries series : jobsMap.values()) {
            if (taskSeriesCollection.getSeriesCount() < plotterConfigurator.getNumberOfSeriesToBeShown()) {
                taskSeriesCollection.add(series);
            }
        }
        LOGGER.info("data set ready");
        return taskSeriesCollection;
    }

    //todo
    public Filter createDataFilter(PlotterConfigurator plotterConfigurator) {
        Filter dataFilter;
        if (plotterConfigurator == null ||  //default
                "task".equalsIgnoreCase(plotterConfigurator.getCategory()) &&
                        "job".equalsIgnoreCase(plotterConfigurator.getColouredDimension())) {

            dataFilter = new SeriesJobsAndTasksTasksFilter();

        } else if ("task".equalsIgnoreCase(plotterConfigurator.getCategory()) &&
                "host".equalsIgnoreCase(plotterConfigurator.getColouredDimension())) {

            dataFilter = new SeriesHostAndTasksTasksFilter();

        } else {
            throw new IllegalArgumentException(
                    "You didn't put the right command line args to start the RunTimesPlotter.");
        }

        return dataFilter;
    }

    public static class SeriesHostAndTasksTasksFilter implements Filter {
        public SeriesHostAndTasksTasksFilter() {
            super();
        }

        @Override
        public Map<String, TaskSeries> filter(List<Trace> traceList) {
            final Map<String, TaskSeries> hostsMap = new TreeMap<String, TaskSeries>();
            for (Trace trace : traceList) {
                if ("m".equals(trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name()))) { // m => tasks of type map
                    final String hostName = trace.getPropertyValue(RunTimesScanner.Keys.HOST.name());
                    final Task taskOnHost = new Task(trace.getId(),   // //categories on category axis
                                                     new Date(trace.getStartTime()),
                                                     new Date(trace.getStopTime()));
                    if (hostsMap.containsKey(hostName)) {
                        hostsMap.get(hostName).add(taskOnHost);
                    } else {
                        final TaskSeries hostTaskSeries = new TaskSeries(hostName);
                        hostTaskSeries.add(taskOnHost);
                        hostsMap.put(hostName, hostTaskSeries);
                    }

                }
            }
            PlotterConfigurator.getInstance().setNumberOfSeries(hostsMap.size());
            doSomeDebugLogging(hostsMap);
            return hostsMap;
        }

        private static void doSomeDebugLogging(Map<String, TaskSeries> hostsMap) {
            for (int i = 1; i <= hostsMap.size(); i++) {
                if (i < 10) {
                    if (hostsMap.get("cvslave0" + i) != null) {
                        System.out.println(
                                "No of tasks in host " + i + ": " + hostsMap.get("cvslave0" + i).getItemCount());
                    }
                } else if (hostsMap.get("cvslave" + i) != null) {
                    System.out.println("No. of tasks in host " + i + ": " + hostsMap.get("cvslave" + i).getItemCount());
                }
            }
        }
    }

    public static class SeriesJobsAndTasksTasksFilter implements Filter {
        public SeriesJobsAndTasksTasksFilter() {
            super();
        }

        @Override
        public Map<String, TaskSeries> filter(List<Trace> traceList) {
            int jobsCounter = 0;
            final Map<String, TaskSeries> jobsMap = new TreeMap<String, TaskSeries>();
            for (Trace trace : traceList) {
                if ("job".equals(trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name()))) {  //job
                    jobsCounter++;
                    final TaskSeries taskSeries = new TaskSeries("job " + trace.getId());
                    jobsMap.put(trace.getId(), taskSeries);
                } else if ("m".equals(trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name()))) { //task of type map
                    if (trace.getId().contains("_m_")) {
                        String jobId = trace.getId().split("_m_")[0];
                        final String taskId = groupTaskId(jobId, trace.getId().split("_m_")[1], false);
                        final TaskSeries jobTaskSeries = jobsMap.get(jobId);

                        final Task task = new Task("task" + taskId, //categories on category axis
                                                   new Date(trace.getStartTime()),
                                                   new Date(trace.getStopTime()));
                        if (null != jobTaskSeries) {
                            jobTaskSeries.add(task);
                        }
                    }
                }
            }
            PlotterConfigurator.getInstance().setNumberOfSeries(jobsCounter);
            return jobsMap;
        }

        private String groupTaskId(String jobId, String taskId, boolean group) {
            if (!group) {
                return taskId;
            }

            //000006_0 -> 6
            final Integer pureTaskIdInteger = Integer.valueOf(taskId.split("_0")[0]);

            /*
            * every 10th task - the first 10 are in one group but only 1 task is plotted
            * why: The taskSeries contains 10 equally called tasks. It overwrites each of them.
            */
            final String groupedTasksId = Integer.valueOf(pureTaskIdInteger / 10).toString();

            /*
            * with modulo (%10) - we get 10 task groups according to the remains of the division by 10
            * The taskSeries contains x equally called tasks sharing the 10 names/ids.
            * Equally called tasks in one series overwrites each other.
            */
//            final String groupedTasksId = Integer.valueOf(pureTaskIdInteger % 10).toString();
            System.out.print("jobId " + jobId + " ");
            System.out.println("taskId " + pureTaskIdInteger + "  task category " + groupedTasksId);
            return groupedTasksId;
        }
    }

    // todo ??? 4th dimension? task, (remote,local), job
    public static class SeriesJobtypeAndTasksTasksFilter implements Filter {
        @Override
        public Map<String, TaskSeries> filter(List<Trace> traceList) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    public interface Filter {

        Map<String, TaskSeries> filter(List<Trace> traceList);
    }
}
