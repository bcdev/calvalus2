package com.bc.calvalus.reporting.urban;

import static org.junit.Assert.*;

import com.bc.calvalus.reporting.code.reader.CursorPosition;
import org.junit.*;
import org.junit.rules.*;

import java.time.LocalDateTime;

/**
 * @author muhammad.bc.
 */
public class CursorPositionTest {

    private static CursorPosition cursorPosition;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Before
    public void setUpEachTest() {
        cursorPosition = new CursorPosition();
    }

    @After
    public void tearDown() {
        cursorPosition.writeLastCursorPosition(LocalDateTime.parse("2017-04-19T08:54:40"));
    }

    @Test
    public void readLastDateTimeCursorPosition() {
        LocalDateTime localDateTime = cursorPosition.readLastCursorPosition();
        LocalDateTime newlocalDateTime = localDateTime.plusHours(1);
        cursorPosition.writeLastCursorPosition(newlocalDateTime);
        assertEquals(localDateTime, LocalDateTime.parse("2017-04-19T08:54:40"));
        assertEquals(cursorPosition.readLastCursorPosition(), LocalDateTime.parse("2017-04-19T09:54:40"));

    }

    @Test
    public void writeNullCursorPosition() {
        expectedException.expect(NullPointerException.class);
        cursorPosition.writeLastCursorPosition(null);
    }


}