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

package com.bc.calvalus.processing.ra;

import com.bc.calvalus.commons.DateRange;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

class RADateRanges {

    private static final DateFormat dateFormat = createDateFormat();

    private static DateFormat createDateFormat() {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        final Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ENGLISH);
        dateFormat.setCalendar(calendar);
        return dateFormat;
    }

    public static RADateRanges create(String dateRangesString) throws ParseException {
        String[] dateRangesStrings = dateRangesString.split(",");
        long[][] dateRanges = new long[dateRangesStrings.length][2];
        for (int i = 0; i < dateRangesStrings.length; i++) {
            String dateRangeString = dateRangesStrings[i];
            DateRange dateRange = DateRange.parseDateRange(dateRangeString);
            dateRanges[i][0] = dateRange.getStartDate().getTime();
            dateRanges[i][1] = dateRange.getStopDate().getTime() + 24 * 60 * 60 * 1000L;
        }
        return new RADateRanges(dateRanges);
    }

    private final long[][] dateRanges;

    private RADateRanges(long[][] dateRanges) {
        this.dateRanges = dateRanges;
    }


    public int findIndex(long time) {
        for (int i = 0; i < dateRanges.length; i++) {
            long[] dateRange = dateRanges[i];
            if (time >= dateRange[0] && time < dateRange[1]) {
                return i;
            }
        }
        return -1;
    }

    public String format(long time) {
        return dateFormat.format(new Date(time));
    }

    public String formatStart(int index) {
        return format(dateRanges[index][0]);
    }

    public String formatEnd(int index) {
        return format(dateRanges[index][1] - 1);
    }

    int numRanges() {
        return dateRanges.length;
    }
}
