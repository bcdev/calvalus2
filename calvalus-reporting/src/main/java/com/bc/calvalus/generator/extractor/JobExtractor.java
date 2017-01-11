package com.bc.calvalus.generator.extractor;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.extractor.jobs.JobsType;
import org.apache.commons.lang.NotImplementedException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.Reader;
import java.util.HashMap;
import java.util.logging.Level;

/**
 * @author muhammad.bc.
 */
public class JobExtractor extends Extractor {
    private final JobsType jobsType;

    public JobExtractor() {
        super();
        String urlJobs = getProperties().getProperty("calvalus.history.jobs.url");
        jobsType = getJobDetail(urlJobs);
    }

    @Override
    public <T> HashMap<String, T> extractInfo(int from, int to, JobsType jobsType) throws GenerateLogException {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public <T> T getType(String jobId) throws JAXBException {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public String getXsltAsString() {
        throw new NotImplementedException("Not implemented");
    }

    public JobsType getJobsType() {
        return jobsType;
    }

    private JobsType getJobDetail(String sourceUrl) {
        try {
            Reader reader = getReadFromSource(sourceUrl);
            JAXBContext jaxbContext = JAXBContext.newInstance(JobsType.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            return (JobsType) unmarshaller.unmarshal(reader);
        } catch (JAXBException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        }
        return null;
    }


}
