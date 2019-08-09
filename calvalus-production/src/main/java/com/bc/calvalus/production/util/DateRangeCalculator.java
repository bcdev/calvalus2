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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calculates date ranges used e.g. for aggregation or statistics.
 */
public class DateRangeCalculator {

    private static final Pattern DAY_REGEX = Pattern.compile("([0-9]+)\\s*d");
    private static final Pattern WEEK_REGEX = Pattern.compile("([0-9]+)\\s*w");
    private static final Pattern MONTH_REGEX = Pattern.compile("([0-9]+)\\s*m");
    private static final Pattern YEAR_REGEX = Pattern.compile("([0-9]+)\\s*y");
    private static final Pattern INT_REGEX = Pattern.compile("([0-9]+)");

    public static List<DateRange> fromDateList(Date... dateList) {
        List<DateRange> dateRangeList = new ArrayList<>();
        for (Date date : dateList) {
            dateRangeList.add(new DateRange(date, date));
        }
        return dateRangeList;
    }

    /**
     * By default the stepping and compositing periods are given in full days.
     * Additional the periods can be specified in weeks (using "w" as suffix),
     * month (using "m" as suffix) or years (using "y" as suffix).
     * <p>
     * The weekly option extends 2 weeks of the year to being 8 days long to
     * get a continuous stepping over multiple years.
     * The week containing the 30 Dec is 8 days long to include the 31 Dec, too.
     * In leap years the week containing the 28 Feb is 8 days long
     * to include the 29 Feb, too.
     * If you dont't want this behaviour you can specify 7 days as period instead.
     * 
     */
    public static List<DateRange> fromMinMax(Date minDate, Date maxDate, String steppingPeriodLength, String compositingPeriodLength) {
        List<DateRange> dateRangeList = new ArrayList<>();

        Period steppingPeriod = parsePeriod(steppingPeriodLength);
        Period compositingPeriod = parsePeriod(compositingPeriodLength);
        final GregorianCalendar calendar = DateUtils.createCalendar();

        // set end of the interval to beginning of the following day for simpler comparison
        calendar.setTime(maxDate);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        final Date end = calendar.getTime();

        calendar.setTime(minDate);
        while (true) {

            // determine start and end of period
            final Date periodStart = calendar.getTime();
            compositingPeriod.next(calendar);
            calendar.add(Calendar.SECOND, -1);
            final Date periodEnd = calendar.getTime();

            // check whether end of period exceeds end of overall interval
            if (periodEnd.after(end)) {
                break;
            }
            // accumulate date range for period
            dateRangeList.add(new DateRange(periodStart, periodEnd));

            // proceed by one period length
            calendar.setTime(periodStart);

            steppingPeriod.next(calendar);
        }
        return dateRangeList;
    }

    static Period parsePeriod(String periodText) {
        if (periodText == null) {
            throw new NullPointerException("periodText");
        }
        periodText = periodText.trim();
        if (periodText.isEmpty()) {
            throw new IllegalArgumentException("periodText is empty");
        }

        Matcher m = DAY_REGEX.matcher(periodText);
        if (m.matches()) {
            return new DayPeriod(Integer.parseInt(m.group(1)));
        }
        m = INT_REGEX.matcher(periodText);
        if (m.matches()) {
            return new DayPeriod(Integer.parseInt(m.group(1)));
        }
        m = WEEK_REGEX.matcher(periodText);
        if (m.matches()) {
            return new WeekPeriod(Integer.parseInt(m.group(1)));
        }
        m = MONTH_REGEX.matcher(periodText);
        if (m.matches()) {
            return new MonthPeriod(Integer.parseInt(m.group(1)));
        }
        m = YEAR_REGEX.matcher(periodText);
        if (m.matches()) {
            return new YearPeriod(Integer.parseInt(m.group(1)));
        }
        if (periodText.equals("-7")) {
            return new WeekPeriod(1);
        }
        if (periodText.equals("-30")) {
            return new MonthPeriod(1);
        }
        throw new IllegalArgumentException("can not parse periodText: " + periodText);
    }

    interface Period {
        void next(GregorianCalendar cal);
    }

    private static abstract class AbstractPeriod implements Period {

        private final int field;
        protected final int period;

        private AbstractPeriod(int field, int period) {
            this.field = field;
            if (period <= 0) {
                throw new IllegalArgumentException("period must be greater than 0, is " + period);
            }
            this.period = period;
        }

        @Override
        public void next(GregorianCalendar cal) {
            cal.add(field, period);
        }
    }

    private static class DayPeriod extends AbstractPeriod {

        private DayPeriod(int period) {
            super(Calendar.DATE, period);
        }

        @Override
        public String toString() {
            return "DayPeriod{" + period + "}";
        }
    }

    private static class WeekPeriod implements Period {

        private final int period;

        private WeekPeriod(int period) {
            if (period <= 0) {
                throw new IllegalArgumentException("period must be greater than 0, is " + period);
            }
            this.period = period;
        }

        @Override
        public void next(GregorianCalendar cal) {
            final int dayOfYearBefore = cal.get(Calendar.DAY_OF_YEAR);
            final boolean isLeapYear = cal.isLeapYear(cal.get(Calendar.YEAR));
            cal.add(Calendar.WEEK_OF_YEAR, period);
            final int dayOfYearAfter = cal.get(Calendar.DAY_OF_YEAR);
            if (isLeapYear) {
                if (dayOfYearBefore < (31 + 28) && dayOfYearAfter > (31 + 28)) {
                    // period that includes 28 Feb includes 29 Feb, too
                    cal.add(Calendar.DATE, 1);
                } else if (dayOfYearBefore < 365 && (dayOfYearAfter > 365 || dayOfYearAfter < dayOfYearBefore)) {
                    // period that includes 30 Dec includes 31 Dec, too
                    cal.add(Calendar.DATE, 1);
                }
            } else if (dayOfYearBefore < 364 && (dayOfYearAfter > 364 || dayOfYearAfter < dayOfYearBefore)) {
                // period that includes 30 Dec includes 31 Dec, too
                cal.add(Calendar.DATE, 1);
            }
        }

        @Override
        public String toString() {
            return "WeekPeriod{" + period + "}";
        }
    }

    private static class MonthPeriod extends AbstractPeriod {

        private MonthPeriod(int period) {
            super(Calendar.MONTH, period);
        }

        @Override
        public String toString() {
            return "MonthPeriod{" + period + "}";
        }
    }

    private static class YearPeriod extends AbstractPeriod {

        private YearPeriod(int period) {
            super(Calendar.YEAR, period);
        }

        @Override
        public String toString() {
            return "YearPeriod{" + period + "}";
        }
    }
}
