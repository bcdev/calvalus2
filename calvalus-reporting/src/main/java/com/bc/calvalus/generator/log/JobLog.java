package com.bc.calvalus.generator.log;


import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.log.jobs.JobsType;

import javax.xml.bind.JAXBException;
import java.util.HashMap;

/**
 * @author muhammad.bc.
 */
public class JobLog extends Log {
    public static final String jobHistoryURL = "http://master00:19888/ws/v1/history/mapreduce/jobs";

    public JobLog() {
        this(jobHistoryURL);
    }

    public JobLog(String sourceUrl) {
        super(sourceUrl);
    }

    @Override
    public <T> HashMap<String, T> extractInfo(JobsType jobsType) throws JAXBException {
        return null;
    }

    @Override
    public <T> HashMap<String, T> extractInfo(int from, int to, JobsType jobsType) throws GenerateLogException, JAXBException {
        return null;
    }

    @Override
    public <T> T getType(String jobId) throws JAXBException {
        return null;
    }

    @Override
    public String getXsltAsString() {
        return null;
    }
}
