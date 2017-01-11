package com.bc.calvalus.generator.extractor;


import static com.bc.calvalus.generator.TestConstants.XMLSourceConf;
import static org.junit.Assert.*;

import com.bc.calvalus.generator.extractor.configuration.Conf;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;

import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;

/**
 * @author muhammad.bc
 */
public class ConfLogTest {
    private static final String PATH = "hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2016/12/21/000004/job_1481485063251_4637_conf.xml";

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");

    }

    @Test
    public void testXMLToConf() throws Exception {
        ConfExtractor log = new ConfExtractor();
        String xsltSourceString = log.getXsltAsString();
        StreamSource xsltSource = new StreamSource(new StringReader(xsltSourceString));
        Conf confType = log.extractInfo(XMLSourceConf, xsltSource, Conf.class);

        assertNotNull(confType);
        assertEquals(confType.getPath().trim(), PATH);
    }

    @Test
    public void testXsltFileAsString() throws Exception {
        ConfExtractor log = new ConfExtractor();
        String xsltAsString = log.getXsltAsString();
        assertNotNull(xsltAsString);
        assertEquals(xsltAsString, log.getXsltAsString());
    }

}