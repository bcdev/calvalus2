package com.bc.calvalus.reporting.extractor.configuration;


import com.bc.calvalus.reporting.extractor.ExtractCalvalusReportException;
import com.bc.calvalus.reporting.extractor.Extractor;
import com.bc.calvalus.reporting.extractor.jobs.JobType;
import com.bc.wps.utilities.PropertiesWrapper;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamSource;

/**
 * @author muhammad.bc.
 */
public class ConfExtractor extends Extractor {
    private static final String CONF_XSL = "conf.xsl";
    private static final String CALVALUS_HISTORY_CONFIGURATION_URL = "calvalus.history.configuration.url";
    private final String urlConf;
    private final String xsltAsString;

    public ConfExtractor() {
        super();
        urlConf = PropertiesWrapper.get(CALVALUS_HISTORY_CONFIGURATION_URL);
        xsltAsString = loadXSLTFile(CONF_XSL);
    }

    @Override
    public <T> HashMap<String, T> extractInfo(int from, int to, List<JobType> jobTypes) throws ExtractCalvalusReportException {
        HashMap<String, Conf> confTypesHashMap = new HashMap<>();
        try {
            for (int i = from; i < to; i++) {
                JobType jobType = jobTypes.get(i);
                String jobTypeId = jobType.getId();
                AtomicReference<Conf> confType = new AtomicReference<>(getType(jobTypeId));
                confTypesHashMap.put(jobTypeId, confType.get());
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
