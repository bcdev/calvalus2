package com.bc.calvalus.reporting.extractor;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author muhammad
 */
@RunWith(ConnectTest.class)
public class ExtractHistoryTest {

    private ClientConnectionToHistory extractHistory;


    @Test(expected = NullPointerException.class)
    public void testLogExtractorURLNull() throws Exception {
        extractHistory = new ClientConnectionToHistory(null);
    }

    @Test
    public void testLogExtractorWrongURL() throws Exception {
        try {
            extractHistory = new ClientConnectionToHistory("localhost.com");
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testLogExtractorURLConnection() throws Exception {
        String logURL = TestUtils.getJobHistoryURL();
        extractHistory = new ClientConnectionToHistory(logURL);
        boolean connect = extractHistory.isConnect();

        assertTrue(connect);
        assertEquals(extractHistory.getSourceUrl(), logURL);
        assertNotNull(extractHistory.getResponse());
        assertEquals(extractHistory.getReadFormatType(), ReadFormatType.XML);

        extractHistory.close();
    }


    @Test
    public void testLogExtractorFormatType() throws Exception {
        String logURL = TestUtils.getJobHistoryURL();
        extractHistory = new ClientConnectionToHistory(logURL, ReadFormatType.JSON);
        boolean connect = extractHistory.isConnect();

        assertTrue(connect);
        assertEquals(extractHistory.getSourceUrl(), logURL);
        assertNotNull(extractHistory.getResponse());
        assertEquals(extractHistory.getReadFormatType(), ReadFormatType.JSON);
        extractHistory.close();
    }

}