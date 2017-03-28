package com.bc.calvalus.extractor;

import com.bc.calvalus.extractor.counter.CounterExtractor;
import com.bc.calvalus.extractor.counter.CounterGroupType;
import com.bc.calvalus.extractor.counter.CountersType;
import org.junit.Test;

import javax.xml.transform.stream.StreamSource;

import java.io.StringReader;

import static com.bc.calvalus.extractor.TestConstants.XMLSourceCounter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author muhammad.bc
 */
public class CounterLogTest {
    private final CounterExtractor log;

    public CounterLogTest() {
        log = new CounterExtractor();
    }


    @Test
    public void testXMLToCounter() throws Exception {
        String xsltSourceString = log.getXsltAsString();
        StreamSource xsltSource = new StreamSource(new StringReader(xsltSourceString));
        CountersType countersType = log.extractInfo(XMLSourceCounter, xsltSource, CountersType.class);

        assertNotNull(countersType);
        assertEquals(countersType.getId(), "job_1481485063251_3649");


        CounterGroupType counterGroup = countersType.getCounterGroup();
        assertNotNull(counterGroup);
        assertEquals(counterGroup.getCounterGroupName(), "org.apache.hadoop.mapreduce.FileSystemCounter");
        assertEquals(counterGroup.getCounter().size(), 2);
    }

    @Test
    public void testExistAndXsltAsString() throws Exception {
        String xsltAsString = log.getXsltAsString();
        assertNotNull(xsltAsString);
    }

}