package com.bc.calvalus.generator.log;


import com.bc.calvalus.generator.ConnectTest;
import com.bc.calvalus.generator.log.configuration.Conf;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;


import static com.bc.calvalus.generator.Constants.XMLSourceConf;
import static com.bc.calvalus.generator.log.JobLog.jobHistoryURL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *@author muhammad.bc
 */
public class ConfLogTest {
    public static final String PATH = "hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2016/12/21/000004/job_1481485063251_4637_conf.xml";



    @Test
    public void testXMLToConf() throws Exception {
        ConfLog log = new ConfLog();
        String xsltSourceString = log.getXsltAsString();
        StreamSource xsltSource = new StreamSource(new StringReader(xsltSourceString));
        Conf confType = log.extractInfo(XMLSourceConf, xsltSource, Conf.class);

        assertNotNull(confType);
        assertEquals(confType.getPath().trim(), PATH);
    }

    @Test
    public void testXsltFileAsString() throws Exception {
        ConfLog log = new ConfLog();
        String xsltAsString = log.getXsltAsString();
        assertNotNull(xsltAsString);
        assertEquals(xsltAsString, log.getXsltAsString());
    }

}