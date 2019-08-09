package com.bc.calvalus.commons;

import org.junit.Test;

import java.text.DateFormat;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Marco Peters
 */
public class DateRangeTest {

    @Test
    public void testToString() throws Exception {
        DateFormat dateFormat = DateRange.DATE_FORMAT;
        Date minDate = dateFormat.parse("1245-04-22");
        Date maxDate = dateFormat.parse("1245-04-23");
        DateRange dateRange = new DateRange(minDate, maxDate);
        assertEquals("[1245-04-22:1245-04-23]", dateRange.toString());
    }

    @Test
    public void testToStringWithNull() throws Exception {
        assertEquals("[null:null]", DateRange.OPEN_RANGE.toString());
    }

    @Test
    public void testParseDateRange() throws Exception {
        DateRange dateRange = DateRange.parseDateRange("[1245-04-22:1245-04-23]");
        DateFormat dateFormat = DateRange.DATE_FORMAT;
        Date expectedStartDate = dateFormat.parse("1245-04-22");
        Date expectedStopDate = dateFormat.parse("1245-04-23");
        assertEquals(expectedStartDate, dateRange.getStartDate());
        assertEquals(expectedStopDate, dateRange.getStopDate());
    }

    @Test
    public void testParseDateRangeWithNull() throws Exception {
        DateRange dateRange = DateRange.parseDateRange("[null:null]");
        assertNull(dateRange.getStartDate());
        assertNull(dateRange.getStopDate());
    }

}
