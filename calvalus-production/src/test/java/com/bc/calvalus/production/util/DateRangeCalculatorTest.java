/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de) 
 *
 * This program is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for 
 * more details.
 *
 * You should have received a copy of the GNU General Public License along 
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.production.util;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.DateUtils;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class DateRangeCalculatorTest {

    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");

    @Test
    public void testFromDateList() throws Exception {
        List<DateRange> dateRanges = DateRangeCalculator.fromDateList();
        assertNotNull(dateRanges);
        assertEquals(0, dateRanges.size());

        dateRanges = DateRangeCalculator.fromDateList(date("2002-01-03"));
        assertNotNull(dateRanges);
        assertEquals(1, dateRanges.size());
        assertEquals("[2002-01-03:2002-01-03]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromDateList(date("2002-01-03"), date("2005-05-05"));
        assertNotNull(dateRanges);
        assertEquals(2, dateRanges.size());
        assertEquals("[2002-01-03:2002-01-03]", dateRanges.get(0).toString());
        assertEquals("[2005-05-05:2005-05-05]", dateRanges.get(1).toString());
    }

    @Test
    public void testParsePeriod() throws Exception {
        assertEquals("DayPeriod{2}", DateRangeCalculator.parsePeriod("2d").toString());
        assertEquals("DayPeriod{2}", DateRangeCalculator.parsePeriod("2").toString());
        try {
            DateRangeCalculator.parsePeriod("0");
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            DateRangeCalculator.parsePeriod("-1");
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            DateRangeCalculator.parsePeriod("2.5");
            fail();
        } catch (IllegalArgumentException ignored) {
        }

        assertEquals("WeekPeriod{1}", DateRangeCalculator.parsePeriod("1w").toString());
        assertEquals("WeekPeriod{1}", DateRangeCalculator.parsePeriod("-7").toString());
        assertEquals("WeekPeriod{3}", DateRangeCalculator.parsePeriod("3w").toString());

        assertEquals("MonthPeriod{1}", DateRangeCalculator.parsePeriod("1m").toString());
        assertEquals("MonthPeriod{1}", DateRangeCalculator.parsePeriod("-30").toString());
        assertEquals("MonthPeriod{3}", DateRangeCalculator.parsePeriod("3m").toString());

        assertEquals("YearPeriod{1}", DateRangeCalculator.parsePeriod("1y").toString());
        assertEquals("YearPeriod{5}", DateRangeCalculator.parsePeriod("5y").toString());

    }

    @Test
    public void testFromMinMax_5_5() throws Exception {
        List<DateRange> dateRanges = DateRangeCalculator.fromMinMax(
                date("2002-04-05"),
                date("2002-04-25"),
                "5",
                "5");

        assertNotNull(dateRanges);
        assertEquals(4, dateRanges.size());
        assertEquals("[2002-04-05:2002-04-09]", dateRanges.get(0).toString());
        assertEquals("[2002-04-10:2002-04-14]", dateRanges.get(1).toString());
        assertEquals("[2002-04-15:2002-04-19]", dateRanges.get(2).toString());
        assertEquals("[2002-04-20:2002-04-24]", dateRanges.get(3).toString());
    }

    @Test
    public void testFromMinMax_5_10() throws Exception {
        List<DateRange> dateRanges = DateRangeCalculator.fromMinMax(
                date("2002-04-05"),
                date("2002-04-25"),
                "5",
                "10");

        assertNotNull(dateRanges);
        assertEquals(3, dateRanges.size());
        assertEquals("[2002-04-05:2002-04-14]", dateRanges.get(0).toString());
        assertEquals("[2002-04-10:2002-04-19]", dateRanges.get(1).toString());
        assertEquals("[2002-04-15:2002-04-24]", dateRanges.get(2).toString());
    }

    @Test
    public void testFromMinMax_10_5() throws Exception {
        List<DateRange> dateRanges = DateRangeCalculator.fromMinMax(
                date("2002-04-05"),
                date("2002-04-25"),
                "10",
                "5");

        assertNotNull(dateRanges);
        assertEquals(2, dateRanges.size());
        assertEquals("[2002-04-05:2002-04-09]", dateRanges.get(0).toString());
        assertEquals("[2002-04-15:2002-04-19]", dateRanges.get(1).toString());
    }

    @Test
    public void testFromMinMax_1w_1w_enfOfFeb() throws Exception {
        List<DateRange> dateRanges = DateRangeCalculator.fromMinMax(
                date("2002-04-05"),
                date("2002-04-25"),
                "1w",
                "1w");
        assertEquals(3, dateRanges.size());
        assertEquals("[2002-04-05:2002-04-11]", dateRanges.get(0).toString());
        assertEquals("[2002-04-12:2002-04-18]", dateRanges.get(1).toString());
        assertEquals("[2002-04-19:2002-04-25]", dateRanges.get(2).toString());

        // 7 vs 1w
        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-02-20"),
                date("2004-03-10"),
                "7",
                "7");
        assertEquals(2, dateRanges.size());
        assertEquals("[2004-02-20:2004-02-26]", dateRanges.get(0).toString());
        assertEquals("[2004-02-27:2004-03-04]", dateRanges.get(1).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-02-20"),
                date("2004-03-10"),
                "1w",
                "1w");
        assertEquals(2, dateRanges.size());
        assertEquals("[2004-02-20:2004-02-26]", dateRanges.get(0).toString());
        assertEquals("[2004-02-27:2004-03-05]", dateRanges.get(1).toString());

        // leap year
        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-02-21"),
                date("2004-03-05"),
                "1w",
                "1w");
        assertEquals("[2004-02-21:2004-02-27]", dateRanges.get(0).toString());
        assertEquals("[2004-02-28:2004-03-05]", dateRanges.get(1).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-02-22"),
                date("2004-03-05"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2004-02-22:2004-02-29]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-02-23"),
                date("2004-03-05"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2004-02-23:2004-03-01]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-02-24"),
                date("2004-03-05"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2004-02-24:2004-03-02]", dateRanges.get(0).toString());

        // no leap year
        dateRanges = DateRangeCalculator.fromMinMax(
                date("2003-02-22"),
                date("2003-03-05"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2003-02-22:2003-02-28]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2003-02-23"),
                date("2003-03-05"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2003-02-23:2003-03-01]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2003-02-24"),
                date("2003-03-05"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2003-02-24:2003-03-02]", dateRanges.get(0).toString());
    }

    @Test
    public void testFromMinMax_1w_1w_enfOfDec() throws Exception {
        List<DateRange> dateRanges;
        // leap year
        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-12-22"),
                date("2005-01-04"),
                "1w",
                "1w");
        assertEquals("[2004-12-22:2004-12-28]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-12-23"),
                date("2005-01-04"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2004-12-23:2004-12-29]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-12-24"),
                date("2005-01-04"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2004-12-24:2004-12-31]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-12-25"),
                date("2005-01-04"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2004-12-25:2005-01-01]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2004-12-26"),
                date("2005-01-04"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2004-12-26:2005-01-02]", dateRanges.get(0).toString());

        // no leap year
        dateRanges = DateRangeCalculator.fromMinMax(
                date("2003-12-22"),
                date("2004-01-04"),
                "1w",
                "1w");
        assertEquals("[2003-12-22:2003-12-28]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2003-12-23"),
                date("2004-01-04"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2003-12-23:2003-12-29]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2003-12-24"),
                date("2004-01-04"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2003-12-24:2003-12-31]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2003-12-25"),
                date("2004-01-04"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2003-12-25:2004-01-01]", dateRanges.get(0).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2003-12-26"),
                date("2004-01-04"),
                "1w",
                "1w");
        assertEquals(1, dateRanges.size());
        assertEquals("[2003-12-26:2004-01-02]", dateRanges.get(0).toString());
    }

    @Test
    public void testFromMinMax_1m_5d() throws Exception {
        List<DateRange> dateRanges = DateRangeCalculator.fromMinMax(
                date("2002-01-01"),
                date("2002-04-30"),
                "1m",
                "5d");
        assertEquals(4, dateRanges.size());
        assertEquals("[2002-01-01:2002-01-05]", dateRanges.get(0).toString());
        assertEquals("[2002-02-01:2002-02-05]", dateRanges.get(1).toString());
        assertEquals("[2002-03-01:2002-03-05]", dateRanges.get(2).toString());
        assertEquals("[2002-04-01:2002-04-05]", dateRanges.get(3).toString());
    }

    @Test
    public void testFromMinMax_1m_1m() throws Exception {
        List<DateRange> dateRanges = DateRangeCalculator.fromMinMax(
                date("2002-01-01"),
                date("2002-04-30"),
                "1m",
                "1m");
        assertEquals(4, dateRanges.size());
        assertEquals("[2002-01-01:2002-01-31]", dateRanges.get(0).toString());
        assertEquals("[2002-02-01:2002-02-28]", dateRanges.get(1).toString());
        assertEquals("[2002-03-01:2002-03-31]", dateRanges.get(2).toString());
        assertEquals("[2002-04-01:2002-04-30]", dateRanges.get(3).toString());

        dateRanges = DateRangeCalculator.fromMinMax(
                date("2002-01-15"),
                date("2002-04-30"),
                "1m",
                "1m");
        assertEquals(3, dateRanges.size());
        assertEquals("[2002-01-15:2002-02-14]", dateRanges.get(0).toString());
        assertEquals("[2002-02-15:2002-03-14]", dateRanges.get(1).toString());
        assertEquals("[2002-03-15:2002-04-14]", dateRanges.get(2).toString());
    }
    
    @Test
    public void testFromMinMax_1y_1y() throws Exception {
        List<DateRange> dateRanges = DateRangeCalculator.fromMinMax(
                date("2002-01-01"),
                date("2004-12-31"),
                "1y",
                "1y");
        assertEquals(3, dateRanges.size());
        assertEquals("[2002-01-01:2002-12-31]", dateRanges.get(0).toString());
        assertEquals("[2003-01-01:2003-12-31]", dateRanges.get(1).toString());
        assertEquals("[2004-01-01:2004-12-31]", dateRanges.get(2).toString());
    }

    private static Date date(String text) throws ParseException {
        return DATE_FORMAT.parse(text);
    }

    @Test
    public void testNext() throws ParseException {
        DateRangeCalculator.Period period = DateRangeCalculator.parsePeriod("-10");
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(DATE_FORMAT.parse("2016-01-01"));
        period.next(cal);
        assertEquals("2016-01-11", DATE_FORMAT.format(cal.getTime()));
        period.next(cal);
        assertEquals("2016-01-21", DATE_FORMAT.format(cal.getTime()));
        period.next(cal);
        assertEquals("2016-02-01", DATE_FORMAT.format(cal.getTime()));
        period.next(cal);
        assertEquals("2016-02-11", DATE_FORMAT.format(cal.getTime()));
        period.next(cal);
        assertEquals("2016-02-21", DATE_FORMAT.format(cal.getTime()));
        period.next(cal);
        assertEquals("2016-03-01", DATE_FORMAT.format(cal.getTime()));
        period.next(cal);
        assertEquals("2016-03-11", DATE_FORMAT.format(cal.getTime()));
    }

    @Test
    public void testPeriodicalDateRanges() throws ParseException {
        String result = DateRangeCalculator.periodicalDateRanges("2020-01-01", "2020-12-31", "1m", "1w");
        assertEquals("[2020-01-01:2020-01-07],[2020-02-01:2020-02-07],[2020-03-01:2020-03-07]," +
                             "[2020-04-01:2020-04-07],[2020-05-01:2020-05-07],[2020-06-01:2020-06-07]," +
                             "[2020-07-01:2020-07-07],[2020-08-01:2020-08-07],[2020-09-01:2020-09-07]," +
                             "[2020-10-01:2020-10-07],[2020-11-01:2020-11-07],[2020-12-01:2020-12-07]", result);
    }
}