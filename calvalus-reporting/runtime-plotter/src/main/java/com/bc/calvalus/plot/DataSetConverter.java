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

    public DataSetConverter() {
        super();
    }

    private RunTimesScanner scanLogFiles() {
        RunTimesScanner runTimesScanner;
        final String fileName = PlotterConfigurator.getInstance().getInputFile();
        try {
            runTimesScanner = new RunTimesScanner(new BufferedReader(new FileReader(fileName)));
            runTimesScanner.scan();
            return runTimesScanner;
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "could not find file " + fileName, e);
            return null;
        }
    }

    public IntervalCategoryDataset createDataSet(Filter dataFilter) {
        final Map<String, TaskSeries> taskSeriesMap = dataFilter.filter(scanLogFiles());
        final TaskSeriesCollection taskSeriesCollection = new TaskSeriesCollection();
        for (TaskSeries series : taskSeriesMap.values()) {
            taskSeriesCollection.add(series);
        }
        return taskSeriesCollection;
    }


    public List<IntervalCategoryDataset> createDataSets(Filter dataFilter) {  //todo subplot part
        final ArrayList<IntervalCategoryDataset> datasets = new ArrayList<IntervalCategoryDataset>();
        datasets.add(new TaskSeriesCollection());
        final Map<String, TaskSeries> jobsMap = dataFilter.filter(scanLogFiles());
//        final TaskSeriesCollection taskSeriesCollection = (TaskSeriesCollection) datasets.get(iterator);
        final PlotterConfigurator plotterConfigurator = PlotterConfigurator.getInstance();
        plotterConfigurator.askForNumberOfSeriesToBeShown();
        int iterator = 0;
        final int seriesToBeShown = plotterConfigurator.getNumberOfSeriesToBeShown();

        for (TaskSeries series : jobsMap.values()) {
            final TaskSeriesCollection taskSeriesCollection = (TaskSeriesCollection) datasets.get(iterator);
            if (taskSeriesCollection.getSeriesCount() < seriesToBeShown) {
                taskSeriesCollection.add(series);
            } else {
                datasets.add(new TaskSeriesCollection());
                iterator = +1;
            }

        }
        LOGGER.info("data set ready");
        return datasets;
    }

    //todo
    public Filter createDataFilter(PlotterConfigurator plotterConfigurator) {
        Filter dataFilter;
        if (plotterConfigurator == null ||
                plotterConfigurator.getCategory() == null ||
                plotterConfigurator.getColouredDimension() == null || //default
                "task".equalsIgnoreCase(plotterConfigurator.getCategory()) &&
                        "job".equalsIgnoreCase(plotterConfigurator.getColouredDimension())) {

            dataFilter = new SeriesJobsAndTasksTasksFilter();

        } else if ("task".equalsIgnoreCase(plotterConfigurator.getCategory()) &&
                "host".equalsIgnoreCase(plotterConfigurator.getColouredDimension())) {

            dataFilter = new SeriesHostAndTasksTasksFilter();
        } else {
            dataFilter = new GeneralPropertiesFilter();
        }
        return dataFilter;
    }

    public static class SeriesHostAndTasksTasksFilter implements Filter {
        private long traceStart;
        private long traceStop;

        @Override
        public Map<String, TaskSeries> filter(RunTimesScanner scanner) {
            final List<Trace> traceList = scanner.getTraces();
            final Map<String, TaskSeries> hostsMap = new TreeMap<String, TaskSeries>();
            for (Trace trace : traceList) {
                // m => tasks of type map
                if ("m".equals(trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name().toLowerCase()))) {
                    if (!fitTraceTimeInTimeInterval(scanner, trace)) {
                        continue;
                    }
                    final String hostName = trace.getPropertyValue(RunTimesScanner.Keys.HOST.name().toLowerCase());
                    final Task taskOnHost = new Task(trace.getId(),   // //categories on category axis
                                                     new Date(traceStart), //trace.getStartTime()
                                                     new Date(traceStop)); //trace.getStopTime()
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

        private boolean fitTraceTimeInTimeInterval(RunTimesScanner scanner, Trace trace) {
            traceStart = trace.getStartTime();
            traceStop = trace.getStopTime();

            final PlotterConfigurator configurator = PlotterConfigurator.getInstance();
            configurator.configureStartAndStop(scanner.getStart(), scanner.getStop());

            if (traceStart != TimeUtils.TIME_NULL && traceStop < configurator.getStart() ||
                    traceStart != TimeUtils.TIME_NULL && traceStart > configurator.getStop()) {
                return false;   //does not fit
            }

            if (traceStart == TimeUtils.TIME_NULL || traceStart < configurator.getStart()) {
                traceStart = configurator.getStart();
            }
            if (traceStop == TimeUtils.TIME_NULL || traceStop > configurator.getStop()) {
                traceStop = configurator.getStop();
            }
            return true;  //fit
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
        private long traceStart;
        private long traceStop;

        @Override
        public Map<String, TaskSeries> filter(RunTimesScanner scanner) {
            final List<Trace> traceList = scanner.getTraces();
            int jobsCounter = 0;
            final Map<String, TaskSeries> jobsMap = new TreeMap<String, TaskSeries>();
            for (Trace trace : traceList) {
                if (!this.fitTraceTimeInTimeInterval(scanner, trace)) {
                    continue;
                }
                if ("job".equals(trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name().toLowerCase()))) {  //job
                    jobsCounter++;
                    final TaskSeries taskSeries = new TaskSeries("job " + trace.getId());
                    jobsMap.put(trace.getId(), taskSeries);
                } else if ("m".equals(
                        trace.getPropertyValue(RunTimesScanner.Keys.TYPE.name().toLowerCase()))) { //task of type map
                    if (trace.getId().contains("_m_")) {
                        String jobId = trace.getId().split("_m_")[0];
                        final String taskId = trace.getId().split("_m_")[1];
                        final TaskSeries jobTaskSeries = jobsMap.get(jobId);

                        final Task task = new Task("task" + taskId, //categories on category axis
                                                   new Date(this.traceStart), //trace.getStartTime()
                                                   new Date(this.traceStop)); //trace.getStopTime()
                        if (null != jobTaskSeries) {
                            jobTaskSeries.add(task);
                        }
                    }
                }
            }
            PlotterConfigurator.getInstance().setNumberOfSeries(jobsCounter);
            return jobsMap;
        }

        private boolean fitTraceTimeInTimeInterval(RunTimesScanner scanner, Trace trace) {
            traceStart = trace.getStartTime();
            traceStop = trace.getStopTime();

            final PlotterConfigurator configurator = PlotterConfigurator.getInstance();
            configurator.configureStartAndStop(scanner.getStart(), scanner.getStop());

            if (traceStart != TimeUtils.TIME_NULL && traceStop < configurator.getStart() ||
                    traceStart != TimeUtils.TIME_NULL && traceStart > configurator.getStop()) {
                return false;   //does not fit
            }
            if (traceStart == TimeUtils.TIME_NULL || traceStart < configurator.getStart()) {
                traceStart = configurator.getStart();
            }
            if (traceStop == TimeUtils.TIME_NULL || traceStop > configurator.getStop()) {
                traceStop = configurator.getStop();
            }
            return true;  //fit
        }
    }

    public static class GeneralPropertiesFilter implements Filter {

        @Override
        public Map<String, TaskSeries> filter(RunTimesScanner scanner) {
            final List<Trace> traceList = scanner.getTraces();
            final PlotterConfigurator configurator = PlotterConfigurator.getInstance();
            final String category = configurator.getCategory();
            final String colour = configurator.getColouredDimension();
            scanner.getValids().add(category, "null");
            scanner.getValids().add(colour, "null");
            final List<String> categoryValids = new ArrayList<String>(scanner.getValids().get(category));
            final List<String> colourValids = new ArrayList<String>(scanner.getValids().get(colour));

            configurator.configureStartAndStop(scanner.getStart(), scanner.getStop());

            final Map<String, TaskSeries> taskSeriesMap = new TreeMap<String, TaskSeries>();
            for (String colourValue : colourValids) {
                TaskSeries series = new TaskSeries(colourValue);
                series.setNotify(false);
                taskSeriesMap.put(colourValue, series);
            }
            for (Trace trace : traceList) {
                final String categoryValue = String.valueOf(trace.getPropertyValue(category));
                final String colourValue = String.valueOf(trace.getPropertyValue(colour));
                final TaskSeries series = taskSeriesMap.get(colourValue);
                series.setNotify(true);
                // check for interval
                long traceStart = trace.getStartTime();
                long traceStop = trace.getStopTime();
                if (traceStart != TimeUtils.TIME_NULL && traceStop < configurator.getStart() ||
                        traceStart != TimeUtils.TIME_NULL && traceStart > configurator.getStop()) {
                    continue;
                }
                if (traceStart == TimeUtils.TIME_NULL || traceStart < configurator.getStart()) {
                    traceStart = configurator.getStart();
                }
                if (traceStop == TimeUtils.TIME_NULL || traceStop > configurator.getStop()) {
                    traceStop = configurator.getStop();
                }
                series.add(new Task(categoryValue, new Date(traceStart), new Date(traceStop)));
            }
            configurator.setNumberOfSeries(taskSeriesMap.size());
            configurator.setNumberOfCategories(categoryValids.size());
            return taskSeriesMap;
        }
    }

    public interface Filter {

        // todo why not returning taskSeriesCollection???
        Map<String, TaskSeries> filter(RunTimesScanner scanner);
    }

    public static abstract class AbstractFilter {
        long traceStart;
        long traceStop;

        abstract Map<String, TaskSeries> filter(RunTimesScanner scanner);

        boolean fitTraceTimeInTimeInterval(RunTimesScanner scanner, Trace trace) {
            traceStart = trace.getStartTime();
            traceStop = trace.getStopTime();

            final PlotterConfigurator configurator = PlotterConfigurator.getInstance();
            configurator.configureStartAndStop(scanner.getStart(), scanner.getStop());

            if (traceStart != TimeUtils.TIME_NULL && traceStop < configurator.getStart() ||
                    traceStart != TimeUtils.TIME_NULL && traceStart > configurator.getStop()) {
                return false;   //does not fit
            }

            if (traceStart == TimeUtils.TIME_NULL || traceStart < configurator.getStart()) {
                traceStart = configurator.getStart();
            }
            if (traceStop == TimeUtils.TIME_NULL || traceStop > configurator.getStop()) {
                traceStop = configurator.getStop();
            }
            return true;  //fit
        }
    }
}
