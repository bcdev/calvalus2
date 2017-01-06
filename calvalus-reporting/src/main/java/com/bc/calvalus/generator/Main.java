package com.bc.calvalus.generator;


import com.bc.calvalus.generator.extractor.JobExtractor;
import com.bc.calvalus.generator.extractor.jobs.JobType;
import com.bc.calvalus.generator.extractor.jobs.JobsType;
import com.bc.calvalus.generator.writer.JobDetailType;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;

/**
 * @author muhammad.bc.
 */
public class Main {

    private String lastJobID;
    private JobsType jobsType;


    public Main(String saveLocation) {
        try {
            JobExtractor jobLog = new JobExtractor();
            jobsType = jobLog.getJobsType();
            lastJobID = getLastJobId(saveLocation);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int[] getRangeIndex(String lastJobID) {

        Optional<JobType> jobTypeOptional = jobsType.getJob().stream().filter(p -> p.getId().equalsIgnoreCase(lastJobID)).findFirst();

        if (!jobTypeOptional.isPresent()) {
            return new int[]{0, jobsType.getJob().size()};
        } else {
            int i = jobsType.getJob().indexOf(jobTypeOptional.get());
            return new int[]{i + 1, jobsType.getJob().size()};
        }
    }

    public String getLastJobID() {
        return lastJobID;
    }

    public JobsType getJobsType() {
        return jobsType;
    }

    private String getLastJobId(String saveLocation) throws IOException {
        String lastLast = null;
        try (
                FileReader fileReader = new FileReader(new File(saveLocation));
                BufferedReader bufferedReader = new BufferedReader(fileReader);
        ) {
            String readLine = null;
            while ((readLine = bufferedReader.readLine()) != null) {
                lastLast = readLine;
            }
        }
        if (lastLast == null) {
            throw new NullPointerException("The file is empty");
        }

        JobDetailType jobDetailType = new Gson().fromJson(lastLast, JobDetailType.class);
        return jobDetailType.getJobId();
    }


}
