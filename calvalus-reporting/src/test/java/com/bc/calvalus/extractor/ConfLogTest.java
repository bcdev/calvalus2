package com.bc.calvalus.extractor;


import com.bc.calvalus.extractor.configuration.Conf;
import com.bc.calvalus.extractor.configuration.ConfExtractor;
import com.bc.wps.utilities.PropertiesWrapper;
import java.io.StringReader;
import javax.xml.transform.stream.StreamSource;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.bc.calvalus.extractor.TestConstants.XMLSourceConf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author muhammad.bc
 */
@Ignore
public class ConfLogTest {
    private static final String PATH = "hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2016/12/21/000004/job_1481485063251_4637_conf.xml";
    private ConfExtractor confExtractor;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-reporting.properties");
        confExtractor = new ConfExtractor();
    }

    @Test
    public void testXMLToConf() throws Exception {
        String xsltSourceString = confExtractor.getXsltAsString();
        StreamSource xsltSource = new StreamSource(new StringReader(xsltSourceString));
        Conf confType = confExtractor.extractInfo(XMLSourceConf, xsltSource, Conf.class);

        assertNotNull(confType);
        assertEquals(confType.getPath().trim(), PATH);
    }

    @Test
    public void testXsltFileAsString() throws Exception {
        String xsltAsString = confExtractor.getXsltAsString();
        assertNotNull(xsltAsString);
        assertEquals(xsltAsString, confExtractor.getXsltAsString());
    }

}