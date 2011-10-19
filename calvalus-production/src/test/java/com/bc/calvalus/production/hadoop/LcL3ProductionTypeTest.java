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


import com.bc.calvalus.production.ProductionRequest;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LcL3ProductionTypeTest {

    @Test
    public void testGetWingsRange() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest(L3ProductionType.NAME, "ewa",
                                                                    "minDate", "2010-06-01",
                                                                    "maxDate", "2010-06-15",
                                                                    "periodLength", "10");
        List<L3ProductionType.DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, 10);
        assertEquals(1, dateRanges.size());
        L3ProductionType.DateRange dateRange = dateRanges.get(0);
        assertEquals("2010-06-01", asString(dateRange.startDate));
        assertEquals("2010-06-10", asString(dateRange.stopDate));

        L3ProductionType.DateRange wingsRange = LcL3ProductionType.getWingsRange(productionRequest, dateRange);
        assertNotNull(wingsRange);
        assertEquals("2010-05-21", asString(wingsRange.startDate));
        assertEquals("2010-06-20", asString(wingsRange.stopDate));
    }

    private static String asString(Date date) {
        return ProductionRequest.getDateFormat().format(date);
    }
}
