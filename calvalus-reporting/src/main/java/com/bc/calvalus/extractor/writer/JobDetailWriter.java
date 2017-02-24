package com.bc.calvalus.extractor.writer;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.extractor.Launcher;
import com.bc.calvalus.extractor.ExtractCalvalusReportException;
import com.bc.calvalus.extractor.configuration.Conf;
import com.bc.calvalus.extractor.configuration.ConfExtractor;
import com.bc.calvalus.extractor.counter.CountersType;
import com.bc.calvalus.extractor.jobs.JobExtractor;
import com.bc.calvalus.extractor.jobs.JobsType;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author muhammad.bc
 */
public class JobDetailWriter {
    private static final int INTERVAL = 10;
    private GetEOFJobInfo getEOFJobInfo;
    private File outputFile;
    private static Logger logger = CalvalusLogger.getLogger();
    private List<JobDetailType> jobDetailTypeList;


    public JobDetailWriter(String pathToWrite) {
        try {
            jobDetailTypeList = new ArrayList<>();
            outputFile = confirmOutputFile(pathToWrite);
            getEOFJobInfo = new GetEOFJobInfo(outputFile);
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    public void start() {
        JobDetailType lastJobInfo = getEOFJobInfo.getLastJobDetailsType();
        List<com.bc.calvalus.extractor.jobs.JobType> sortedJobTypeList = sortedJobsType();
        if (lastJobInfo == null) {
            writeWithIntervals(sortedJobTypeList);
        } else {
            List<com.bc.calvalus.extractor.jobs.JobType> filterAfterDate = sortedJobTypeList
                    .stream()
                    .filter(p -> lastJobInfo.compareTo(p.getFinishTime()) == -1)
                    .collect(Collectors.toList());

            writeWithIntervals(filterAfterDate);
        }
    }

    private void writeWithIntervals(List<com.bc.calvalus.extractor.jobs.JobType> sortedJobTypeList) {
        int start;
        int stop = 0;
        int size = sortedJobTypeList.size();
        AtomicInteger atomicInteger = new AtomicInteger();
        try {
            while (true) {
                start = atomicInteger.get();
                stop = atomicInteger.addAndGet(INTERVAL);

                if (!(stop <= size)) {
                    stop = atomicInteger.addAndGet(-INTERVAL);
                    break;
                }
                mergeConfCounterRecord(start, stop, sortedJobTypeList);
            }
            mergeConfCounterRecord(stop, size, sortedJobTypeList);
        } catch (ExtractCalvalusReportException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }


    private void mergeConfCounterRecord(int from, int to, List<com.bc.calvalus.extractor.jobs.JobType> sortedJobTypeList) throws ExtractCalvalusReportException {
        int interval = to - from;
        if (interval <= 0) {
            return;
        }
        logger.log(Level.INFO, String.format("Start extracting %d configuration and counter history from hadoop web service.", interval));
        HashMap<String, Conf> confLog = createConfLog(from, to, sortedJobTypeList);
        HashMap<String, CountersType> counterLog = createCounterLog(from, to, sortedJobTypeList);
        write(confLog, counterLog, sortedJobTypeList);
    }

    private void write(HashMap<String, Conf> confInfo, HashMap<String, CountersType> counterInfo, List<com.bc.calvalus.extractor.jobs.JobType> jobTypeList) throws ExtractCalvalusReportException {
        if (confInfo.size() != counterInfo.size()) {
            throw new ExtractCalvalusReportException("The size of the configuration and counter history have different size");
        }
        for (com.bc.calvalus.extractor.jobs.JobType jobType : jobTypeList) {
            String jobId = jobType.getId();
            Conf conf = confInfo.get(jobId);
            CountersType countersType = counterInfo.get(jobId);
            if (conf != null && countersType != null) {
                logger.log(Level.INFO, String.format("Writing %s job detail to cache memory", jobId));
                addJobDetails(conf, countersType, jobType);
            }
        }
        flushToFile(jobDetailTypeList, outputFile);
        jobDetailTypeList.clear();
    }

    void flushToFile(List<JobDetailType> jobDetailTypeList, File outputFile) {
        try (
                FileWriter fileWriter = new FileWriter(outputFile, true);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)
        ) {
            Gson gson = new Gson();
            for (JobDetailType jobDetailType : jobDetailTypeList) {
                bufferedWriter.append(gson.toJson(jobDetailType));
                bufferedWriter.write(",");
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
        logger.log(Level.INFO, String.format("Flush all cache job details to %s", outputFile.toString()));
    }

    public static void stop() {
        Thread thread = Thread.currentThread();
        Launcher.terminate = true;
        thread.interrupt();
    }

    private File confirmOutputFile(String pathToWrite) throws IOException {
        File file = Paths.get(pathToWrite).toFile();
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

    private void addJobDetails(Conf conf, CountersType countersType, com.bc.calvalus.extractor.jobs.JobType jobType) {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setJobInfo(jobType);
        jobDetailType.setConfInfo(conf);
        jobDetailType.setCounterInfo(countersType);
        jobDetailTypeList.add(jobDetailType);
    }


    private HashMap<String, CountersType> createCounterLog(int from, int to, List<com.bc.calvalus.extractor.jobs.JobType> sortedJobTypeList) throws ExtractCalvalusReportException {
        com.bc.calvalus.extractor.counter.CounterExtractor counterLog = new com.bc.calvalus.extractor.counter.CounterExtractor();
        return counterLog.extractInfo(from, to, sortedJobTypeList);
    }

    private HashMap<String, Conf> createConfLog(int from, int to, List<com.bc.calvalus.extractor.jobs.JobType> sortedJobTypeList) throws ExtractCalvalusReportException {
        ConfExtractor confLog = new ConfExtractor();
        return confLog.extractInfo(from, to, sortedJobTypeList);
    }


    private List<com.bc.calvalus.extractor.jobs.JobType> sortedJobsType() {
        JobExtractor jobExtractor = new JobExtractor();
        JobsType jobsTypeList = jobExtractor.getJobsType();

        List<com.bc.calvalus.extractor.jobs.JobType> listOfJobs = jobsTypeList.getJob();
        Comparator<com.bc.calvalus.extractor.jobs.JobType> jobTypeComparator = (o1, o2) -> {
            Instant dateTime1 = Instant.ofEpochMilli(Long.parseLong(o1.getFinishTime()));
            Instant dateTime2 = Instant.ofEpochMilli(Long.parseLong(o2.getFinishTime()));
            if (dateTime1.isAfter(dateTime2)) {
                return 1;
            }
            if (dateTime1.isBefore(dateTime2)) {
                return -1;
            }
            return 0;
        };
        Collections.sort(listOfJobs, jobTypeComparator);
        return listOfJobs;
    }

    static class GetEOFJobInfo {
        private JobDetailType lastJobID;

        GetEOFJobInfo(File saveLocation) {
            try {
                lastJobID = getLastJobId(saveLocation);
            } catch (IOException | JsonSyntaxException e) {

                if (e instanceof JsonSyntaxException) {
                    String msg = String.format("The last line JSON is not well formatted in :%s\n%s", saveLocation.toPath(), e.getMessage());
                    logger.log(Level.SEVERE, msg);
                    stop();
                } else {
                    logger.log(Level.SEVERE, e.getMessage());
                }
            }
        }


        JobDetailType getLastJobDetailsType() {
            return lastJobID;
        }

        private JobDetailType getLastJobId(File saveLocation) throws IOException, JsonSyntaxException {
            String lastLine = null;
            JobDetailType jobDetailType;
            try (
                    FileReader fileReader = new FileReader(saveLocation);
                    BufferedReader bufferedReader = new BufferedReader(fileReader)
            ) {
                String readLine;
                while ((readLine = bufferedReader.readLine()) != null) {
                    lastLine = readLine;
                }

                if (lastLine == null || lastLine.isEmpty()) {
                    return null;
                }
                jobDetailType = new Gson().fromJson(lastLine.replace("},", "}"), JobDetailType.class);
            }
            return jobDetailType;
        }
    }
}
