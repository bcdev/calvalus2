/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.commons;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * This class represents a data range.
 */
public class DateRange {

    public static final DateRange OPEN_RANGE = new DateRange(null, null);
    public static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");

    private static final String SEPARATOR = ":";
    private static final String CLOSING_BRACKET = "]";
    private static final String OPENING_BRACKET = "[";

    private final Date startDate;
    private final Date stopDate;

    public DateRange(Date startDate, Date stopDate) {
        this.startDate = startDate;
        this.stopDate = stopDate;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getStopDate() {
        return stopDate;
    }

    @Override
    public String toString() {
        String startString = startDate != null ? DATE_FORMAT.format(startDate) : "null";
        String stopString = stopDate != null ? DATE_FORMAT.format(stopDate) : "null";
        return OPENING_BRACKET + startString + SEPARATOR + stopString + CLOSING_BRACKET;
    }

    public static DateRange parseDateRange(String dateRange) throws ParseException {
        String[] splits = dateRange.split(SEPARATOR);
        String startString = splits[0].replace(OPENING_BRACKET, "");
        String stopString = splits[1].replace(CLOSING_BRACKET, "");
        return new DateRange(parseDate(startString), parseDate(stopString));
    }

    private static Date parseDate(String date) throws ParseException {
        if (date.equals("null")) {
            return null;
        } else {
            final DateFormat dateFormat = DateUtils.createDateFormat("yyyy-MM-dd");
            return dateFormat.parse(date);
        }
    }
}
