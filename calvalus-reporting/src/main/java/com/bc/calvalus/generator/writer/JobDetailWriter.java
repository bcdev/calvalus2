package com.bc.calvalus.generator.writer;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.Launcher;
import com.bc.calvalus.generator.extractor.ConfExtractor;
import com.bc.calvalus.generator.extractor.CounterExtractor;
import com.bc.calvalus.generator.extractor.JobExtractor;
import com.bc.calvalus.generator.extractor.configuration.Conf;
import com.bc.calvalus.generator.extractor.counter.CounterGroupType;
import com.bc.calvalus.generator.extractor.counter.CounterType;
import com.bc.calvalus.generator.extractor.counter.CountersType;
import com.bc.calvalus.generator.extractor.jobs.JobType;
import com.bc.calvalus.generator.extractor.jobs.JobsType;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;

/**
 * @author muhammad.bc
 */
public class JobDetailWriter {


    private static final int INTERVAL = 10;
    private final GetJobInfo getJobInfo;
    private JobsType jobsTypeList;
    private final List<JobDetailType> jobDetailTypeList;
    private final File outputFile;
    private final Logger logger;


    public JobDetailWriter(String pathToWrite) {
        logger = CalvalusLogger.getLogger();
        jobDetailTypeList = new ArrayList<>();
        outputFile = confirmOutputFile(pathToWrite);
        getJobInfo = new GetJobInfo(outputFile);
    }

    private void write(int from, int to) throws JAXBException, GenerateLogException {
        int last = from;
        for (int i = from; i < to; i++) {
            if (i % INTERVAL == 0) {
                if (last != i) {
                    write_(last, i);
                    last = i + 1;
                }
            }
        }
        write_(last, to);
    }

    private String getLastJobID() {
        return getJobInfo.getLastJobID();
    }


    public void start() {
        try {
            String lastJobID = getLastJobID();
            if (lastJobID == null) {
                write(0, getJobType().getJob().size());
            } else {
                int[] startStopIndex = getStartStopIndex(lastJobID);
                write(startStopIndex[0], startStopIndex[1]);
            }
        } catch (JAXBException | GenerateLogException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private void stop() {
        Thread thread = Thread.currentThread();
        if (thread != null) {
            Launcher.terminate = true;
            thread.interrupt();
        }
    }

    private int[] getStartStopIndex(String lastJobID) {
        List<JobType> listOfJobs = getJobType().getJob();
        Optional<JobType> jobTypeOptional = listOfJobs.stream().filter(p -> p.getId().equalsIgnoreCase(lastJobID)).findFirst();
        if (!jobTypeOptional.isPresent()) {
            return new int[]{0, listOfJobs.size()};
        } else {
            int i = listOfJobs.indexOf(jobTypeOptional.get());
            return new int[]{i + 1, listOfJobs.size()};
        }
    }

    private void write_(int from, int to) throws GenerateLogException {
        HashMap<String, Conf> confLog = createConfLog(from, to);
        HashMap<String, CountersType> counterLog = createCounterLog(from, to);
        write(confLog, counterLog);
    }

    private JobsType getJobType() {
        if (jobsTypeList == null) {
            JobExtractor jobExtractor = new JobExtractor();
            jobsTypeList = jobExtractor.getJobsType();
        }
        return jobsTypeList;
    }

    private File confirmOutputFile(String pathToWrite) {
        Path path = Paths.get(pathToWrite);
        File file = path.toFile();
        if (!file.exists()) {
            try {
                throw new FileNotFoundException("The folder does not exist.");
            } catch (FileNotFoundException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
        }
        return file;
    }

    private void write(HashMap<String, Conf> confInfo, HashMap<String, CountersType> counterInfo) throws GenerateLogException {
        List<JobType> jobTypeList = getJobType().getJob();
        if (confInfo.size() != counterInfo.size()) {
            throw new GenerateLogException("The size of the configuration and counter history have different size");
        }
        for (JobType jobType : jobTypeList) {
            String jobId = jobType.getId();

            Conf conf = confInfo.get(jobId);
            CountersType countersType = counterInfo.get(jobId);
            if (conf != null && countersType != null) {
                addJobDetails(conf, countersType, jobType);
            }
        }
        flushToFile();
    }

    private void addJobDetails(Conf conf, CountersType countersType, JobType jobType) {
        JobDetailType jobDetailType = new JobDetailType();
        writeJobs(jobType, jobDetailType);
        writeConf(conf, jobDetailType);
        writeCounter(countersType, jobDetailType);
        jobDetailTypeList.add(jobDetailType);
    }

    private void writeJobs(JobType jobType, JobDetailType jobDetailType) {
        jobDetailType.setJobId(jobType.getId());
        jobDetailType.setUser(jobType.getUser());
        jobDetailType.setQueue(jobType.getQueue());
        jobDetailType.setStartTime(jobType.getStartTime());
        jobDetailType.setFinishTime(jobType.getFinishTime());
        jobDetailType.setMapsCompleted(jobType.getMapsCompleted());
        jobDetailType.setReducesCompleted(jobType.getReducesCompleted());
        jobDetailType.setState(jobType.getState());
    }

    private void writeCounter(CountersType conf, JobDetailType jobDetailType) {
        CounterGroupType counterGroup = conf.getCounterGroup();
        List<CounterType> counter = counterGroup.getCounter();
        for (CounterType counterType : counter) {
            addCounterInfo(counterType, jobDetailType);
        }
    }

    private void writeConf(Conf conf, JobDetailType jobDetailType) {
        String path = conf.getPath();
        jobDetailType.setInputPath(path);
    }

    private void addCounterInfo(CounterType counterType, JobDetailType jobDetailType) {
        String counterTypeName = counterType.getName();

        if (counterTypeName.equalsIgnoreCase("FILE_BYTES_READ")) {
            jobDetailType.setFileBytesRead(counterType.getTotalCounterValue().toString());
        } else if (counterTypeName.equalsIgnoreCase("FILE_BYTES_WRITTEN")) {
            jobDetailType.setFileBytesWritten(counterType.getTotalCounterValue().toString());
        } else if (counterTypeName.equalsIgnoreCase("HDFS_BYTES_READ")) {
            jobDetailType.setHdfsBytesRead(counterType.getTotalCounterValue().toString());
        } else if (counterTypeName.equalsIgnoreCase("HDFS_BYTES_WRITTEN")) {
            jobDetailType.setHdfsBytesWritten(counterType.getTotalCounterValue().toString());
        } else if (counterTypeName.equalsIgnoreCase("VCORES_MILLIS_MAPS")) {
            jobDetailType.setvCoresMillisTotal(counterType.getTotalCounterValue().toString());
        } else if (counterTypeName.equalsIgnoreCase("MB_MILLIS_MAPS")) {
            jobDetailType.setMbMillisTotal(counterType.getTotalCounterValue().toString());
        } else if (counterTypeName.equalsIgnoreCase("CPU_MILLISECONDS")) {
            jobDetailType.setCpuMilliseconds(counterType.getTotalCounterValue().toString());
        }
    }

    private void flushToFile() {
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
            jobDetailTypeList.clear();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private HashMap<String, CountersType> createCounterLog(int from, int to) throws GenerateLogException {
        CounterExtractor counterLog = new CounterExtractor();
        JobsType jobsType = getJobType();
        return counterLog.extractInfo(from, to, jobsType);
    }

    private HashMap<String, Conf> createConfLog(int from, int to) throws GenerateLogException {
        ConfExtractor confLog = new ConfExtractor();
        JobsType jobsType = getJobType();
        return confLog.extractInfo(from, to, jobsType);
    }

    private class GetJobInfo {
        private String lastJobID;

        public GetJobInfo(File saveLocation) {
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


        public String getLastJobID() {
            return lastJobID;
        }

        private String getLastJobId(File saveLocation) throws IOException, JsonSyntaxException {
            String lastLine = null;
            JobDetailType jobDetailType = null;
            try (
                    FileReader fileReader = new FileReader(saveLocation);
                    BufferedReader bufferedReader = new BufferedReader(fileReader)
            ) {
                String readLine;
                while ((readLine = bufferedReader.readLine()) != null) {
                    lastLine = readLine;
                }

                if (lastLine == null || lastLine.isEmpty()) {
                    return lastLine;
                }
                jobDetailType = new Gson().fromJson(lastLine.replace("},", "}"), JobDetailType.class);
            }
            return jobDetailType.getJobId();
        }
    }


}
