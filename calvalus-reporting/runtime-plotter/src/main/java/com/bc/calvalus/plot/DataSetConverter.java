package com.bc.calvalus.plot;

import org.jfree.data.category.IntervalCategoryDataset;
import org.jfree.data.gantt.Task;
import org.jfree.data.gantt.TaskSeries;
import org.jfree.data.gantt.TaskSeriesCollection;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataSetConverter {
    private final static Logger LOGGER = Logger.getAnonymousLogger();

    private TaskSeriesCollection taskSeriesCollection;

    public DataSetConverter() {
        super();
        taskSeriesCollection = new TaskSeriesCollection();
    }

    // todo remove
    // only to play around and look on scan results in file
    public static void main(String[] args) {
        new DataSetConverter().fetchInputLogFiles();
    }

    private List<Trace> scanLogFiles() {
        final String userHomeTemp = System.getProperty("user.home") + "/temp/";
        final String fileName = "hadoop-hadoop-jobtracker-cvmaster00.log.2010-10-28";
        RunTimesScanner runTimesScanner;
        try {
            runTimesScanner = new RunTimesScanner(new BufferedReader(new FileReader(userHomeTemp + fileName)));
            return runTimesScanner.scan();
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "could not find file in " + userHomeTemp, e);
            return null;
        }
    }

    // todo remove
    //just to analyse the results -- not a part of the code
    void fetchInputLogFiles() {
        final String userHomeTemp = System.getProperty("user.home") + "/temp/";
        final String fileName = "hadoop-hadoop-jobtracker-cvmaster00.log.2010-10-28";
        RunTimesScanner runTimesScanner;
        BufferedWriter bufferedWriter;
        try {
            runTimesScanner = new RunTimesScanner(new BufferedReader(new FileReader(userHomeTemp + fileName)));
            runTimesScanner.scan();

            //report into a file
            bufferedWriter = new BufferedWriter(new FileWriter(userHomeTemp + "RunTimeScannerResults.txt"));
            bufferedWriter.write("scanner start: " + runTimesScanner.start + "\n");
            bufferedWriter.write("scanner stop: " + runTimesScanner.stop + "\n");
            for (Trace trace : runTimesScanner.getTraces()) {
                bufferedWriter.write((trace.isOpen() ? "*" : "") + trace.toString() + "\n");
            }
            bufferedWriter.write(runTimesScanner.getValids().toString() + "\n");
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "could not find file in " + userHomeTemp, e);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "error opening FileWriter ", e);
        }
    }

    public IntervalCategoryDataset createDataSet(Filter dataFilter) {
        final Map<String, TaskSeries> jobsMap = dataFilter.filter(scanLogFiles());
        for (TaskSeries series : jobsMap.values()) {
            taskSeriesCollection.add(series);
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

            dataFilter = new SeriesJobsAndTasksTasksFilter(PlotterConfigurator.askForNumberOfJobsToBeShown());

        } else if ("task".equalsIgnoreCase(plotterConfigurator.getCategory()) &&
                "host".equalsIgnoreCase(plotterConfigurator.getColouredDimension())) {

            dataFilter = new SeriesHostAndTasksTasksFilter(PlotterConfigurator.askForNumberOfHostsToBeShown());

        } else {
            throw new IllegalArgumentException(
                    "You didn't put the right command line args to start the RunTimesPlotter.");
        }

        return dataFilter;
    }

    public static class SeriesHostAndTasksTasksFilter implements Filter {
        private int MAX_HOSTS_SHOWN;

        public SeriesHostAndTasksTasksFilter(int maxHostsShown) {
            super();
            this.MAX_HOSTS_SHOWN = maxHostsShown;
        }

        @Override
        public Map<String, TaskSeries> filter(List<Trace> traceList) {
            final HashMap<String, TaskSeries> hostsMap = new HashMap<String, TaskSeries>();
            for (Trace trace : traceList) {
                if ("m".equals(trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name()))) { // m equates tasks
                    final String hostName = trace.getPropertyValue(RunTimesScanner.Keys.HOST.name());
                    final Task taskOnHost = new Task(trace.getId(),   // //categories on category axis
                                                     new Date(trace.getStartTime()),
                                                     new Date(trace.getStopTime()));
                    if (hostsMap.containsKey(hostName)) {
                        hostsMap.get(hostName).add(taskOnHost);
                    } else {
                        if (hostsMap.size() < MAX_HOSTS_SHOWN) {
                            final TaskSeries hostTaskSeries = new TaskSeries(hostName);
                            hostTaskSeries.add(taskOnHost);
                            hostsMap.put(hostName, hostTaskSeries);
                        }
                    }

                }
            }
            doSomeDebugLogging(hostsMap);
            return hostsMap;
        }

        private static void doSomeDebugLogging(HashMap<String, TaskSeries> hostsMap) {
            LOGGER.info("Number of the hosts found in the log file: " + hostsMap.size());
//            for (int i = 1; i <= hostsMap.size(); i++) {
//                if (i < 10) {
//                    System.out.println("No of tasks in host " + i + ": " + hostsMap.get("cvslave0" + i).getItemCount());
//                } else {
//                    System.out.println("No. of tasks in host " + i + ": " + hostsMap.get("cvslave" + i).getItemCount());
//                }
//            }
        }
    }

    public static class SeriesJobsAndTasksTasksFilter implements Filter {
        private int MAX_JOBS_SHOWN = 1;

        public SeriesJobsAndTasksTasksFilter(int maxJobsShown) {
            super();
            MAX_JOBS_SHOWN = maxJobsShown;
        }

        @Override
        public Map<String, TaskSeries> filter(List<Trace> traceList) {
            int jobsCounter = 0;
            final HashMap<String, TaskSeries> jobsMap = new HashMap<String, TaskSeries>();
            for (Trace trace : traceList) {
                if ("job".equals(trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name()))) {  //job
                    jobsCounter++;
                    if (jobsMap.size() < MAX_JOBS_SHOWN) { //job
                        final TaskSeries taskSeries = new TaskSeries("job " + trace.getId());
                        jobsMap.put(trace.getId(), taskSeries);
                    }
                } else if ("m".equals(trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name()))) { //task
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
            LOGGER.info("Number of the jobs found in the log file: " + jobsCounter);
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
            if (Integer.valueOf(jobId.split("_")[1]) < MAX_JOBS_SHOWN) {
                System.out.print("jobId " + jobId + " ");
                System.out.println("taskId " + pureTaskIdInteger + "  task category " + groupedTasksId);
            }
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
