package com.bc.calvalus.urban;

import com.bc.calvalus.code.de.reader.CodeDeException;
import java.time.LocalDateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

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
    public void tearDown() throws Exception {
        cursorPosition.writeLastCursorPosition(LocalDateTime.parse("2017-04-19T08:54:40"));
    }

    @Test
    public void readLastDateTimeCursorPosition() throws CodeDeException {
        LocalDateTime localDateTime = cursorPosition.readLastCursorPosition();
        LocalDateTime newlocalDateTime = localDateTime.plusHours(1);
        cursorPosition.writeLastCursorPosition(newlocalDateTime);
        assertEquals(localDateTime,LocalDateTime.parse("2017-04-19T08:54:40"));
        assertEquals(cursorPosition.readLastCursorPosition(),LocalDateTime.parse("2017-04-19T09:54:40"));

    }

    @Test
    public void writeNullCursorPosition() throws Exception {
        expectedException.expect(NullPointerException.class);
        cursorPosition.writeLastCursorPosition(null);
    }


}