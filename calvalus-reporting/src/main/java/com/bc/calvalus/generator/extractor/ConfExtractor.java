package com.bc.calvalus.generator.extractor;


import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.extractor.configuration.Conf;
import com.bc.calvalus.generator.extractor.jobs.JobType;
import com.bc.calvalus.generator.extractor.jobs.JobsType;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;

/**
 * @author muhammad.bc.
 */
public class ConfExtractor extends Extractor {

    public static final String HTTP_MASTER_WS_V1_HISTORY_MAPREDUCE_JOBS_CONF = "http://master00:19888/ws/v1/history/mapreduce/jobs/%s/configuration";
    public static final String CONF_XSL = "conf.xsl";
    private String xsltAsString;

    public ConfExtractor() {
        super();
        xsltAsString = loadXSLTFile(CONF_XSL);
    }

    public ConfExtractor(String sourceUrl) {
        super(sourceUrl);
        xsltAsString = loadXSLTFile(CONF_XSL);
    }


    @Override
    public <T> HashMap<String, T> extractInfo(JobsType jobsType) {
        HashMap<String, Conf> confTypesHashMap = new HashMap<>();
        try {
            for (JobType jobType : jobsType.getJob()) {
                String jobTypeId = jobType.getId();
                Conf confType = getType(jobTypeId);
                confTypesHashMap.put(jobTypeId, confType);
            }
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return (HashMap<String, T>) confTypesHashMap;
    }

    @Override
    public <T> HashMap<String, T> extractInfo(int from, int to, JobsType jobsType) throws GenerateLogException {
        HashMap<String, Conf> confTypesHashMap = new HashMap<>();
        List<JobType> jobTypes = jobsType.getJob();

        int size = jobTypes.size();

        if (!(size >= from && from >= 0) || !(size >= to && to >= 0) || !(to >= from)) {
            throw new GenerateLogException("The range is out of bound");
        }

        try {
            for (int i = from; i < to; i++) {
                JobType jobType = jobTypes.get(i);
                String jobTypeId = jobType.getId();
                Conf confType = getType(jobTypeId);
                confTypesHashMap.put(jobTypeId, confType);
            }
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return (HashMap<String, T>) confTypesHashMap;
    }

    public Conf getType(String jobId) throws JAXBException {
        StreamSource xsltSource = new StreamSource(new StringReader(xsltAsString));
        String sourceURL = String.format(HTTP_MASTER_WS_V1_HISTORY_MAPREDUCE_JOBS_CONF, jobId);
        return extractInfo(sourceURL, xsltSource, Conf.class);
    }

    public String getXsltAsString() {
        return xsltAsString;
    }
}
