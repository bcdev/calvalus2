package com.bc.calvalus.generator.extractor;


import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.extractor.configuration.Conf;
import com.bc.calvalus.generator.extractor.jobs.JobType;
import com.bc.calvalus.generator.extractor.jobs.JobsType;
import com.bc.wps.utilities.PropertiesWrapper;
import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;

/**
 * @author muhammad.bc.
 */
public class ConfExtractor extends Extractor {
    private static final String CONF_XSL = "conf.xsl";
    private final String urlConf;
    private final String xsltAsString;

    public ConfExtractor() {
        super();
        urlConf = PropertiesWrapper.get("calvalus.history.configuration.url");
        xsltAsString = loadXSLTFile(CONF_XSL);
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
        String sourceURL = String.format(urlConf, jobId);
        return extractInfo(sourceURL, xsltSource, Conf.class);
    }

    public String getXsltAsString() {
        return xsltAsString;
    }
}
