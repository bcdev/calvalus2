package com.bc.calvalus.generator.extractor;


import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.extractor.jobs.JobsType;

import javax.xml.bind.JAXBException;
import java.util.HashMap;

/**
 * @author muhammad.bc.
 */
public class JobExtractor extends Extractor {
    public static final String jobHistoryURL = "http://master00:19888/ws/v1/history/mapreduce/jobs";

    public JobExtractor() {
        this(jobHistoryURL);
    }

    public JobExtractor(String sourceUrl) {
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
