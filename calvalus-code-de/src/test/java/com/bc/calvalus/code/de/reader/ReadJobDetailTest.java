package com.bc.calvalus.code.de.reader;

import com.bc.wps.utilities.PropertiesWrapper;
import java.time.LocalDateTime;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author muhammad.bc.
 */
@Ignore
public class ReadJobDetailTest {

    @BeforeAll
    public static void testOnceBefereAllTest() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/code-de.properties");
    }

    @Test
    public void testCursorPosition() throws CodeDeException {
        ReadJobDetail.CursorPosition cursorPosition = new ReadJobDetail.CursorPosition();
        LocalDateTime now = LocalDateTime.now();
        cursorPosition.writeLastCursorPosition(now);
        LocalDateTime readLastCursorPosition = cursorPosition.readLastCursorPosition();

        assertNotNull(readLastCursorPosition);
        assertEquals(readLastCursorPosition.toString(), now.toString());
    }

    @Test
    void DateTime() {
        LocalDateTime parse = LocalDateTime.parse("2017-02-01T00:40:00.019");
        System.out.println("parse.toString() = " + parse.toString());

    }
}