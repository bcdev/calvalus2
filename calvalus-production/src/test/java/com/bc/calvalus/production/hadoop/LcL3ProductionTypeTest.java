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

package com.bc.calvalus.production.hadoop;


import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LcL3ProductionTypeTest {

    @Test
    public void testGetDatePairList_MinMax_10() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest(LcL3ProductionType.NAME, "ewa",
                                                                    "minDate", "2010-06-01",
                                                                    "maxDate", "2010-08-31",
                                                                    "periodLength", "10");

        List<L3ProductionType.DateRange> dateRangeList = LcL3ProductionType.getDateRanges(productionRequest, 10);
        assertEquals(9, dateRangeList.size());
        assertEquals("2010-06-01", asString(dateRangeList.get(0).startDate));
        assertEquals("2010-06-10", asString(dateRangeList.get(0).stopDate));

        assertEquals("2010-06-11", asString(dateRangeList.get(1).startDate));
        assertEquals("2010-06-20", asString(dateRangeList.get(1).stopDate));

        assertEquals("2010-06-21", asString(dateRangeList.get(2).startDate));
        assertEquals("2010-06-30", asString(dateRangeList.get(2).stopDate));

        assertEquals("2010-07-01", asString(dateRangeList.get(3).startDate));
        assertEquals("2010-07-10", asString(dateRangeList.get(3).stopDate));

        assertEquals("2010-07-11", asString(dateRangeList.get(4).startDate));
        assertEquals("2010-07-20", asString(dateRangeList.get(4).stopDate));

        assertEquals("2010-07-21", asString(dateRangeList.get(5).startDate));
        assertEquals("2010-07-31", asString(dateRangeList.get(5).stopDate));

        assertEquals("2010-08-01", asString(dateRangeList.get(6).startDate));
        assertEquals("2010-08-10", asString(dateRangeList.get(6).stopDate));

        assertEquals("2010-08-11", asString(dateRangeList.get(7).startDate));
        assertEquals("2010-08-20", asString(dateRangeList.get(7).stopDate));

        assertEquals("2010-08-21", asString(dateRangeList.get(8).startDate));
        assertEquals("2010-08-31", asString(dateRangeList.get(8).stopDate));

    }

    @Test
    public void testGetDatePairList_MinMax_15() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest(LcL3ProductionType.NAME, "ewa",
                                                                    "minDate", "2010-07-01",
                                                                    "maxDate", "2010-07-31",
                                                                    "periodLength", "15");

        List<L3ProductionType.DateRange> dateRangeList = LcL3ProductionType.getDateRanges(productionRequest, 10);
        assertEquals(2, dateRangeList.size());

        assertEquals("2010-07-01", asString(dateRangeList.get(0).startDate));
        assertEquals("2010-07-15", asString(dateRangeList.get(0).stopDate));

        assertEquals("2010-07-16", asString(dateRangeList.get(1).startDate));
        assertEquals("2010-07-31", asString(dateRangeList.get(1).stopDate));
    }

    @Test
    public void testGetWingsRange() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest(LcL3ProductionType.NAME, "ewa", "wings", "10");

        L3ProductionType.DateRange dateRange = new L3ProductionType.DateRange(asDate("2010-06-01"), asDate("2010-06-10"));

        L3ProductionType.DateRange wingsRange = LcL3ProductionType.getWingsRange(productionRequest, dateRange);
        assertNotNull(wingsRange);
        assertEquals("2010-05-22", asString(wingsRange.startDate));
        assertEquals("2010-06-20", asString(wingsRange.stopDate));
    }

    private static String asString(Date date) {
        return ProductionRequest.getDateFormat().format(date);
    }

    private static Date asDate(String date) throws ParseException {
        return ProductionRequest.getDateFormat().parse(date);
    }
}
