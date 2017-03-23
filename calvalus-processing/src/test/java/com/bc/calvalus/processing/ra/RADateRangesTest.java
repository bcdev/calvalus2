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

import org.junit.Test;

import static org.junit.Assert.*;

public class RADateRangesTest {

    @Test
    public void testCreate() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10");
        assertEquals(1, dateRanges.numRanges());

        dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-10:2010-01-20");
        assertEquals(2, dateRanges.numRanges());
    }

    @Test
    public void testFormatStartEnd() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20");

        assertEquals("2010-01-01 00:00:00", dateRanges.formatStart(0));
        assertEquals("2010-01-10 23:59:59", dateRanges.formatEnd(0));

        assertEquals("2010-01-11 00:00:00", dateRanges.formatStart(1));
        assertEquals("2010-01-20 23:59:59", dateRanges.formatEnd(1));
    }

    @Test
    public void testfindIndex() throws Exception {
        RADateRanges dateRanges = RADateRanges.create("2010-01-01:2010-01-10,2010-01-11:2010-01-20");
        //
        long a = 1263168000000L; // 2010-01-11 00:00:00.000
        assertEquals(0, dateRanges.findIndex(a - 2));
        assertEquals(0, dateRanges.findIndex(a - 1));
        assertEquals(1, dateRanges.findIndex(a));
        assertEquals(1, dateRanges.findIndex(a + 1));
        assertEquals(1, dateRanges.findIndex(a + 2));
    }

}