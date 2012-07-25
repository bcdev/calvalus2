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


import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.production.DateRange;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class LcL3ProductionTypeTest {

    @Test
    public void testGetDatePairList_period7() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest(LcL3ProductionType.NAME, "ewa",
                                                                    "minDate", "2010-06-01",
                                                                    "maxDate", "2010-06-30",
                                                                    "periodLength", "7");

        List<DateRange> dateRangeList = LcL3ProductionType.getDateRanges(productionRequest, 7);
        assertEquals(4, dateRangeList.size());
        assertEquals("2010-06-01", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-06-07", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-06-08", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-06-14", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-06-15", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-06-21", asString(dateRangeList.get(2).getStopDate()));

        assertEquals("2010-06-22", asString(dateRangeList.get(3).getStartDate()));
        assertEquals("2010-06-30", asString(dateRangeList.get(3).getStopDate()));
    }


    @Test
    public void testGetDatePairList_period10() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest(LcL3ProductionType.NAME, "ewa",
                                                                    "minDate", "2010-06-01",
                                                                    "maxDate", "2010-08-31",
                                                                    "periodLength", "10");

        List<DateRange> dateRangeList = LcL3ProductionType.getDateRanges(productionRequest, 10);
        assertEquals(9, dateRangeList.size());
        assertEquals("2010-06-01", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-06-10", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-06-11", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-06-20", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-06-21", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-06-30", asString(dateRangeList.get(2).getStopDate()));

        assertEquals("2010-07-01", asString(dateRangeList.get(3).getStartDate()));
        assertEquals("2010-07-10", asString(dateRangeList.get(3).getStopDate()));

        assertEquals("2010-07-11", asString(dateRangeList.get(4).getStartDate()));
        assertEquals("2010-07-20", asString(dateRangeList.get(4).getStopDate()));

        assertEquals("2010-07-21", asString(dateRangeList.get(5).getStartDate()));
        assertEquals("2010-07-31", asString(dateRangeList.get(5).getStopDate()));

        assertEquals("2010-08-01", asString(dateRangeList.get(6).getStartDate()));
        assertEquals("2010-08-10", asString(dateRangeList.get(6).getStopDate()));

        assertEquals("2010-08-11", asString(dateRangeList.get(7).getStartDate()));
        assertEquals("2010-08-20", asString(dateRangeList.get(7).getStopDate()));

        assertEquals("2010-08-21", asString(dateRangeList.get(8).getStartDate()));
        assertEquals("2010-08-31", asString(dateRangeList.get(8).getStopDate()));
    }

    @Test
    public void testGetDatePairList_period15() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest(LcL3ProductionType.NAME, "ewa",
                                                                    "minDate", "2010-07-01",
                                                                    "maxDate", "2010-07-31",
                                                                    "periodLength", "15");

        List<DateRange> dateRangeList = LcL3ProductionType.getDateRanges(productionRequest, 10);
        assertEquals(2, dateRangeList.size());

        assertEquals("2010-07-01", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-07-15", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-07-16", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-07-31", asString(dateRangeList.get(1).getStopDate()));
    }

    @Test
    public void testGetWingsRange() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest(LcL3ProductionType.NAME, "ewa", "wings", "10");

        DateRange dateRange = new DateRange(asDate("2010-06-01"), asDate("2010-06-10"));

        DateRange wingsRange = LcL3ProductionType.getWingsRange(productionRequest, dateRange);
        assertNotNull(wingsRange);
        assertEquals("2010-05-22", asString(wingsRange.getStartDate()));
        assertEquals("2010-06-20", asString(wingsRange.getStopDate()));
    }

    @Test
    public void testgetDateRange() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest(LcL3ProductionType.NAME, "ewa",
                                                                    "minDate", "2010-07-01",
                                                                    "periodLength", "15");

        DateRange dateRange = LcL3ProductionType.getDateRange_OLD(productionRequest);
        assertNotNull(dateRange);
        assertEquals("2010-07-01", asString(dateRange.getStartDate()));
        assertEquals("2010-07-15", asString(dateRange.getStopDate()));
    }

    @Test
    public void testGetCloudL3Config() throws Exception {
        L3Config cloudL3Config = LcL3ProductionType.getCloudL3Config(new ProductionRequest("test", "dummy", "foo", "bar"));
        assertEquals("status == 1 and not nan(sdr_8)", cloudL3Config.getMaskExpr());

        cloudL3Config = LcL3ProductionType.getCloudL3Config(new ProductionRequest("test", "dummy", "calvalus.lc.remapAsLand", "10"));
        assertEquals("(status == 1  or status == 10) and not nan(sdr_8)", cloudL3Config.getMaskExpr());

        cloudL3Config = LcL3ProductionType.getCloudL3Config(new ProductionRequest("test", "dummy", "calvalus.lc.remapAsLand", "10,11,20"));
        assertEquals("(status == 1  or status == 10 or status == 11 or status == 20) and not nan(sdr_8)", cloudL3Config.getMaskExpr());
    }

    @Test
    public void testGetLcPeriodName() throws Exception {
        String periodName = LcL3ProductionType.getLcPeriodName(new ProductionRequest("test", "ewa",
                                                                                   "minDate", "2010-07-01",
                                                                                   "maxDate", "2010-07-15"));
        assertEquals("FR-2010-07-01-15d", periodName);

        periodName = LcL3ProductionType.getLcPeriodName(new ProductionRequest("test", "ewa",
                                                                            "minDate", "2010-07-01",
                                                                            "maxDate", "2010-07-07"));
        assertEquals("FR-2010-07-01-7d", periodName);

        periodName = LcL3ProductionType.getLcPeriodName(new ProductionRequest("test", "ewa",
                                                                            "minDate", "2010-07-01",
                                                                            "maxDate", "2010-07-10"));
        assertEquals("FR-2010-07-01-10d", periodName);

        periodName = LcL3ProductionType.getLcPeriodName(new ProductionRequest("test", "ewa",
                                                                            "minDate", "2011-01-01",
                                                                            "maxDate", "2011-12-31"));
        assertEquals("FR-2011-01-01-365d", periodName);

        // periodLength is NOT used
        periodName = LcL3ProductionType.getLcPeriodName(new ProductionRequest("test", "ewa",
                                                                            "minDate", "2010-03-02",
                                                                            "maxDate", "2010-03-12",
                                                                            "periodLength", "20",
                                                                            "resolution", "RR"));
        assertEquals("RR-2010-03-02-11d", periodName);

        try {
            LcL3ProductionType.getLcPeriodName(new ProductionRequest("test", "ewa",
                                                                   "minDate", "2010-03-02"));
            fail();
        } catch (ProductionException pe) {
            assertEquals("Production parameter 'maxDate' not set.", pe.getMessage());
        }

    }

    private static String asString(Date date) {
        return ProductionRequest.getDateFormat().format(date);
    }

    private static Date asDate(String date) throws ParseException {
        return ProductionRequest.getDateFormat().parse(date);
    }
}
