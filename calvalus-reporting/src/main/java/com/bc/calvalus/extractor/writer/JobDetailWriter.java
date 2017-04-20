package com.bc.calvalus.extractor.writer;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.extractor.ExtractCalvalusReportException;
import com.bc.calvalus.extractor.Launcher;
import com.bc.calvalus.extractor.configuration.Conf;
import com.bc.calvalus.extractor.configuration.ConfExtractor;
import com.bc.calvalus.extractor.counter.CounterExtractor;
import com.bc.calvalus.extractor.counter.CountersType;
import com.bc.calvalus.extractor.jobs.JobExtractor;
import com.bc.calvalus.extractor.jobs.JobType;
import com.bc.calvalus.extractor.jobs.JobsType;
import com.bc.wps.utilities.PropertiesWrapper;
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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author muhammad.bc
 */
public class JobDetailWriter {
    private static final int FIRST_DAY_OF_MONTH = 1;
    private static Logger logger = CalvalusLogger.getLogger();
    private GetEOFJobInfo getEOFJobInfo;
    private List<JobDetailType> jobDetailTypeList = new ArrayList<>();
    private File outputFile;
    private String outputPath;
    private int interval;

    public JobDetailWriter() {
        initProperties();
    }

    public static void stop() {
        Thread thread = Thread.currentThread();
        Launcher.terminate = true;
        thread.interrupt();
    }

    public void start() {
        Map<String, List<JobType>> sortedJobsGroupByDate = sortedJobsType()
                .stream()
                .collect(Collectors.groupingBy(p -> getFirstDateFromMilliseconds(p)));

        sortedJobsGroupByDate.forEach((startDateAsFileName, jobTypes) -> {
            setOutputFilePath(startDateAsFileName);
            getEOFJobInfo = new GetEOFJobInfo(outputFile);
            JobDetailType lastJobInfo = getEOFJobInfo.getLastJobDetailsType();

            if (lastJobInfo == null) {
                writeWithIntervals(jobTypes);
            } else {
                List<JobType> filterAfterDate = jobTypes
                        .stream()
                        .filter(p -> lastJobInfo.compareTo(p.getFinishTime()) == -1)
                        .collect(Collectors.toList());
                if (filterAfterDate.size() == 0) {
                    return;
                }
                writeWithIntervals(filterAfterDate);

            }
        });
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

    private void initProperties() {
        outputPath = PropertiesWrapper.get("reporting.folder.path");
        interval = Integer.parseInt(PropertiesWrapper.get("write.log.interval"));
    }

    private void setOutputFilePath(String startDateAsFileName) {
        try {
            outputFile = confirmOutputFile(startDateAsFileName);
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private String getFirstDateFromMilliseconds(JobType p) {
        LocalDate firstDayOfMonth = null;
        ZonedDateTime zonedDateTime = Instant.ofEpochMilli(Long.parseLong(p.getFinishTime())).atZone(ZoneOffset.UTC);
        firstDayOfMonth = zonedDateTime.toLocalDate().withDayOfMonth(FIRST_DAY_OF_MONTH);
        return firstDayOfMonth.toString();
    }


    private void writeWithIntervals(List<JobType> sortedJobTypeList) {
        int start;
        int stop = 0;
        int size = sortedJobTypeList.size();
        AtomicInteger atomicInteger = new AtomicInteger();
        try {
            while (true) {
                start = atomicInteger.get();
                stop = atomicInteger.addAndGet(interval);

                if (!(stop <= size)) {
                    stop = atomicInteger.addAndGet(-interval);
                    break;
                }
                writeConfCounterRecord(start, stop, sortedJobTypeList);
            }
            writeConfCounterRecord(stop, size, sortedJobTypeList);
        } catch (ExtractCalvalusReportException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private void writeConfCounterRecord(int from, int to, List<JobType> sortedJobTypeList) throws ExtractCalvalusReportException {
        int interval = to - from;
        if (interval <= 0) {
            return;
        }
        logger.log(Level.INFO, String.format("Start extracting %d configuration and counter history from hadoop web service.", interval));
        HashMap<String, Conf> confLog = createConfLog(from, to, sortedJobTypeList);
        HashMap<String, CountersType> counterLog = createCounterLog(from, to, sortedJobTypeList);
        write(confLog, counterLog, sortedJobTypeList);
    }

    private void write(HashMap<String, Conf> confInfo, HashMap<String, CountersType> counterInfo, List<JobType> jobTypeList) throws ExtractCalvalusReportException {
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

    private File confirmOutputFile(String startDateAsFileName) throws IOException {
        String fileNameFormat = fileNameFormat(startDateAsFileName);

        File file = Paths.get(outputPath).resolve(fileNameFormat).toFile();
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

    private String fileNameFormat(String firstDayDateMonth) {
        String lastDayDate = LocalDate
                .parse(firstDayDateMonth)
                .plusMonths(1)
                .withDayOfMonth(FIRST_DAY_OF_MONTH)
                .minusDays(1)
                .toString();

        return String.format("calvalus-reporting-%s-to-%s.json", firstDayDateMonth, lastDayDate);
    }

    private void addJobDetails(Conf conf, CountersType countersType, JobType jobType) {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setJobInfo(jobType);
        jobDetailType.setConfInfo(conf);
        jobDetailType.setCounterInfo(countersType);
        jobDetailTypeList.add(jobDetailType);
    }

    private HashMap<String, CountersType> createCounterLog(int from, int to, List<JobType> sortedJobTypeList) throws ExtractCalvalusReportException {
        CounterExtractor counterLog = new CounterExtractor();
        return counterLog.extractInfo(from, to, sortedJobTypeList);
    }

    private HashMap<String, Conf> createConfLog(int from, int to, List<JobType> sortedJobTypeList) throws ExtractCalvalusReportException {
        ConfExtractor confLog = new ConfExtractor();
        return confLog.extractInfo(from, to, sortedJobTypeList);
    }

    private List<JobType> sortedJobsType() {
        JobExtractor jobExtractor = new JobExtractor();
        JobsType jobsTypeList = jobExtractor.getJobsType();
        List<JobType> listOfJobs = jobsTypeList.getJob();
        Comparator<JobType> jobTypeComparator = (o1, o2) -> {
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
