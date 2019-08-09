package com.bc.calvalus.reporting.extractor;

import com.bc.calvalus.reporting.extractor.counter.CounterExtractor;
import com.bc.calvalus.reporting.extractor.counter.CounterGroupType;
import com.bc.calvalus.reporting.extractor.counter.CountersType;
import com.bc.wps.utilities.PropertiesWrapper;
import java.io.IOException;
import java.io.StringReader;
import javax.xml.transform.stream.StreamSource;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.bc.calvalus.reporting.extractor.TestConstants.XMLSourceCounter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author muhammad.bc
 */
@Ignore
public class CounterLogTest {
    private CounterExtractor counterExtractor;

    @Before
    public void CounterLogTest() {
        try {
            PropertiesWrapper.loadConfigFile("calvalus-reporting.properties");
            counterExtractor = new CounterExtractor();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testXMLToCounter() throws Exception {
        String xsltSourceString = counterExtractor.getXsltAsString();
        StreamSource xsltSource = new StreamSource(new StringReader(xsltSourceString));
        CountersType countersType = counterExtractor.extractInfo(XMLSourceCounter, xsltSource, CountersType.class);

        assertNotNull(countersType);
        assertEquals(countersType.getId(), "job_1481485063251_3649");


        CounterGroupType counterGroup = countersType.getCounterGroup();
        assertNotNull(counterGroup);
        assertEquals(counterGroup.getCounterGroupName(), "org.apache.hadoop.mapreduce.FileSystemCounter");
        assertEquals(counterGroup.getCounter().size(), 2);
    }

    @Test
    public void testExistAndXsltAsString() throws Exception {
        String xsltAsString = counterExtractor.getXsltAsString();
        assertNotNull(xsltAsString);
    }

}