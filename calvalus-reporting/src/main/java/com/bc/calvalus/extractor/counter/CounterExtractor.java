package com.bc.calvalus.extractor.counter;


import com.bc.calvalus.extractor.ExtractCalvalusReportException;
import com.bc.calvalus.extractor.Extractor;
import com.bc.wps.utilities.PropertiesWrapper;
import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;

/**
 * @author muhammad.bc.
 */
public class CounterExtractor extends Extractor {


    private static final String COUNTER_XSL = "counter.xsl";
    private final String countersUrl;
    private final String xsltAsString;

    public CounterExtractor() {
        countersUrl = PropertiesWrapper.get("calvalus.history.counters.url");
        xsltAsString = loadXSLTFile(COUNTER_XSL);
    }

    @Override
    public <T> HashMap<String, T> extractInfo(int from, int to, List<com.bc.calvalus.extractor.jobs.JobType> jobTypes) throws ExtractCalvalusReportException {
        HashMap<String, CountersType> confTypesHashMap = new HashMap<>();
        int size = jobTypes.size();
        if (!(size >= from && from >= 0) && !(size >= to && to >= 0) && (to >= from)) {
            throw new ExtractCalvalusReportException("The range is out of bound");
        }
        try {
            for (int i = from; i < to; i++) {
                com.bc.calvalus.extractor.jobs.JobType jobType = jobTypes.get(i);
                String jobTypeId = jobType.getId();
                CountersType confType = getType(jobTypeId);
                confTypesHashMap.put(jobTypeId, confType);
            }
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return (HashMap<String, T>) confTypesHashMap;
    }


    public CountersType getType(String jobId) throws JAXBException {
        StreamSource xsltSource = new StreamSource(new StringReader(xsltAsString));
        String sourceURL = String.format(countersUrl, jobId);
        return extractInfo(sourceURL, xsltSource, CountersType.class);
    }

    public String getXsltAsString() {
        return xsltAsString;
    }
}
