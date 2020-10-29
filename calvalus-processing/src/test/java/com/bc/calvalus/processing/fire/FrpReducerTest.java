package com.bc.calvalus.processing.fire;

import org.junit.Test;

import java.util.Date;

import static com.bc.calvalus.commons.DateUtils.createCalendar;
import static org.junit.Assert.assertEquals;

public class FrpReducerTest {

    @Test
    public void testGetDateTime() {
        final Date date = new Date(1603948559000L);

        final String[] dateTime= FrpReducer.getDateTime(date, createCalendar());
        assertEquals(2, dateTime.length);
        assertEquals("20201029", dateTime[0]);
        assertEquals("0515", dateTime[1]);
    }
}
