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

package com.bc.calvalus.commons;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Methods for handling dates.
 * Takes care of using UTC
 */
public class DateUtils {

    public static final String ISO_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    public static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");
    public static final DateFormat ISO_FORMAT = createDateFormat(ISO_FORMAT_STRING);
    public static final DateFormat DATE_FORMAT = createDateFormat(DATE_FORMAT_STRING);

    public static GregorianCalendar createCalendar() {
        final GregorianCalendar calendar = new GregorianCalendar(UTC_TIME_ZONE, Locale.ENGLISH);
        calendar.clear();
        return calendar;
    }
    
    public static SimpleDateFormat createDateFormat(String pattern) {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(pattern, Locale.ENGLISH);
        dateFormat.setCalendar(createCalendar());
        return dateFormat;
    }

    public static String formatDate(Date date) {
        return DATE_FORMAT.format(date);
    }
}
