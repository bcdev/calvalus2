package com.bc.calvalus.code.de.reader;

/**
 * @author muhammad.bc.
 */
class CursorPositionTest {
/*

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
*/





}