package com.bc.calvalus.code.de.reader;

import com.bc.wps.utilities.PropertiesWrapper;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author muhammad.bc.
 */
public class ReadJobDetailTest {

    @BeforeAll
    public static void testOnceBefereAllTest() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/code-de.properties");
    }

    @Test
    public void testCursorPosition() throws Exception {
        ReadJobDetail.EntryCursor entryCursor = new ReadJobDetail.EntryCursor();
        LocalDateTime now = LocalDateTime.now();
        entryCursor.writeLastCursorPosition(now);
        LocalDateTime readLastCursorPosition = entryCursor.readLastCursorPosition();

        assertNotNull(readLastCursorPosition);
        assertEquals(readLastCursorPosition.toString(), now.toString());
    }

    @Test
    public void testCreateQuery() throws Exception {
        LocalDateTime localDateTime = LocalDateTime.now();
        String codeDeUrl = PropertiesWrapper.get("code.de.url");
        String format = String.format(codeDeUrl + "%s/%s", localDateTime.toString(), localDateTime.toString());

        ReadJobDetail readFromSource = new ReadJobDetail();
        String query = readFromSource.createURL(localDateTime);
        assertEquals(query, format);

    }
}