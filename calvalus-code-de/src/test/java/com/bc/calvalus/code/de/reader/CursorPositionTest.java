package com.bc.calvalus.code.de.reader;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * @author muhammad.bc.
 */
class CursorPositionTest {

    private static ReadJobDetail.CursorPosition cursorPosition;

    @BeforeEach
    void setUpEachTest() {
        cursorPosition = new ReadJobDetail.CursorPosition();
        cursorPosition.deleteSerializeFile();
    }


    @Test
    void readLastCursorPosition() throws CodeDeException {
        LocalDateTime actualWithoutSeconds = LocalDateTime.now().minusMinutes(5);
        LocalDateTime readLastCursorPosition = cursorPosition.readLastCursorPosition();
        String actual = actualWithoutSeconds.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
        String expect = readLastCursorPosition.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
        assertEquals(expect, actual);
    }

    @Test
    void writeLastDateTimeCursorPosition() throws CodeDeException {
        LocalDateTime localDateTime = LocalDateTime.now();
        cursorPosition.writeLastCursorPosition(localDateTime);
        LocalDateTime readLastDateTimeFromSerializeFile = cursorPosition.readLastCursorPosition();
        assertEquals(localDateTime, readLastDateTimeFromSerializeFile);
    }

    @Test
    void writeNullCursorPosition() throws Exception {
        assertThrows(NullPointerException.class, () -> cursorPosition.writeLastCursorPosition(null));
    }





}