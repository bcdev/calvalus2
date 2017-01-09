package com.bc.calvalus.generator.extractor;

import com.bc.calvalus.generator.ConnectTest;
import com.bc.calvalus.generator.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author muhammad
 */
@RunWith(ConnectTest.class)
public class LogReaderTest {

    private LogReader logReader;


    @Test(expected = NullPointerException.class)
    public void testLogExtractorURLNull() throws Exception {
        logReader = new LogReader(null);
    }

    @Test
    public void testLogExtractorWrongURL() throws Exception {
        try {
            logReader = new LogReader("localhost.com");
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testLogExtractorURLConnection() throws Exception {
        String logURL = TestUtils.getJobHistoryURL();
        logReader = new LogReader(logURL);
        boolean connect = logReader.isConnect();

        assertTrue(connect);
        assertEquals(logReader.getSourceUrl(), logURL);
        assertNotNull(logReader.getRawSource());
        assertEquals(logReader.getFormatType(), FormatType.XML);

        logReader.close();
    }


    @Test
    public void testLogExtractorFormatType() throws Exception {
        String logURL = TestUtils.getJobHistoryURL();
        logReader = new LogReader(logURL, FormatType.JSON);
        boolean connect = logReader.isConnect();

        assertTrue(connect);
        assertEquals(logReader.getSourceUrl(), logURL);
        assertNotNull(logReader.getRawSource());
        assertEquals(logReader.getFormatType(), FormatType.JSON);
        logReader.close();
    }

}