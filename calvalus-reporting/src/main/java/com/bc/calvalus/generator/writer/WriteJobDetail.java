package com.bc.calvalus.generator.writer;


import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.log.ConfLog;
import com.bc.calvalus.generator.log.CounterLog;
import com.bc.calvalus.generator.log.JobLog;
import com.bc.calvalus.generator.log.configuration.Conf;
import com.bc.calvalus.generator.log.counter.CounterGroupType;
import com.bc.calvalus.generator.log.counter.CounterType;
import com.bc.calvalus.generator.log.counter.CountersType;
import com.bc.calvalus.generator.log.jobs.JobType;
import com.bc.calvalus.generator.log.jobs.JobsType;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class WriteJobDetail {


    private List<JobDetailType> jobDetailTypeList;
    private File outputFile;

    public WriteJobDetail(String pathToWrite) {
        jobDetailTypeList = new ArrayList<>();
        outputFile = confirmOutputFile(pathToWrite);
    }

    private File confirmOutputFile(String pathToWrite) {
        Path path = Paths.get(pathToWrite);
        File file = path.toFile();
        if (!file.exists()) {
            try {
                throw new FileNotFoundException("The folder does not exist.");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return file;
    }

    public void writeAll() throws JAXBException {
        JobLog jobLog = new JobLog();
        List<JobType> jobTypeList = jobLog.getJobsType().getJob();

        ConfLog confLog = new ConfLog();
        CounterLog counterLog = new CounterLog();

        HashMap<String, Conf> confInfo = confLog.extractInfo(confLog.getJobsType());
        HashMap<String, CountersType> counterInfo = counterLog.extractInfo(counterLog.getJobsType());

        for (int i = 0; i < jobTypeList.size(); i++) {
            JobType jobType = jobTypeList.get(i);
            String jobId = jobType.getId();
            Conf conf = confInfo.get(jobId);
            CountersType countersType = counterInfo.get(jobId);
            addJobDetails(conf, countersType, jobType);
        }
        flushToFile();
    }


    public void write(String jobId) throws JAXBException {
        JobLog jobLog = new JobLog();
        JobsType jobsType = jobLog.getJobsType();
        Optional<JobType> jobTypeOptional = jobsType.getJob().stream().filter(p -> p.getId().equalsIgnoreCase(jobId)).findFirst();
        JobType jobType = null;
        if (jobTypeOptional.isPresent()) {
            jobType = jobTypeOptional.get();
        } else {
            throw new IllegalArgumentException("The job id does not exist");
        }

        ConfLog confLog = new ConfLog();
        Conf conf = confLog.getType(jobId);

        CounterLog counterLog = new CounterLog();
        CountersType countersType = counterLog.getType(jobId);

        addJobDetails(conf, countersType, jobType);
        flushToFile();
    }

    public void write(int range, JobsType jobsType) throws JAXBException, GenerateLogException {
        write(0, range, jobsType);
    }

    public void write(int from, int to, JobsType jobsType) throws JAXBException, GenerateLogException {
        HashMap<String, Conf> confLog = createConfLog(from, to, jobsType);
        HashMap<String, CountersType> counterLog = createCounterLog(from, to, jobsType);
        write(confLog, counterLog, jobsType);
    }

    private void write(HashMap<String, Conf> confInfo, HashMap<String, CountersType> counterInfo, JobsType jobsType) throws GenerateLogException {
        List<JobType> jobTypeList = jobsType.getJob();
        if (confInfo.size() != counterInfo.size()) {
            throw new GenerateLogException("The size of the configuration is not the same with the counter");
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
        writeToFile();
    }

    private void writeToFile() {
        try (
                FileWriter fileWriter = new FileWriter(outputFile, true);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        ) {

            for (JobDetailType jobDetailType : jobDetailTypeList) {
                bufferedWriter.append(jobDetailType.toString());
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HashMap<String, CountersType> createCounterLog(int from, int to, JobsType jobsType) throws JAXBException, GenerateLogException {
        CounterLog counterLog = new CounterLog();
        return counterLog.extractInfo(from, to, jobsType);
    }

    private HashMap<String, Conf> createConfLog(int from, int to, JobsType jobsType) throws JAXBException, GenerateLogException {
        ConfLog confLog = new ConfLog();
        return confLog.extractInfo(from, to, jobsType);
    }

}
