package com.bc.calvalus.generator.log;

import com.bc.calvalus.generator.log.counter.CounterGroupType;
import com.bc.calvalus.generator.log.counter.CountersType;
import org.junit.Test;

import javax.xml.transform.stream.StreamSource;

import java.io.StringReader;

import static com.bc.calvalus.generator.Constants.XMLSourceCounter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Author ubits on 1/3/2017.
 */
public class CounterLogTest {
    private final CounterLog log;

    public CounterLogTest() {
        log = new CounterLog();
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