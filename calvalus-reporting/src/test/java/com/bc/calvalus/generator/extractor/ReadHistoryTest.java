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
public class ReadHistoryTest {

    private ReadHistory readHistory;


    @Test(expected = NullPointerException.class)
    public void testLogExtractorURLNull() throws Exception {
        readHistory = new ReadHistory(null);
    }

    @Test
    public void testLogExtractorWrongURL() throws Exception {
        try {
            readHistory = new ReadHistory("localhost.com");
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testLogExtractorURLConnection() throws Exception {
        String logURL = TestUtils.getJobHistoryURL();
        readHistory = new ReadHistory(logURL);
        boolean connect = readHistory.isConnect();

        assertTrue(connect);
        assertEquals(readHistory.getSourceUrl(), logURL);
        assertNotNull(readHistory.getRawSource());
        assertEquals(readHistory.getReadFormatType(), ReadFormatType.XML);

        readHistory.close();
    }


    @Test
    public void testLogExtractorFormatType() throws Exception {
        String logURL = TestUtils.getJobHistoryURL();
        readHistory = new ReadHistory(logURL, ReadFormatType.JSON);
        boolean connect = readHistory.isConnect();

        assertTrue(connect);
        assertEquals(readHistory.getSourceUrl(), logURL);
        assertNotNull(readHistory.getRawSource());
        assertEquals(readHistory.getReadFormatType(), ReadFormatType.JSON);
        readHistory.close();
    }

}