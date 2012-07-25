/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.production;

import org.junit.Test;

import static org.junit.Assert.*;

public class DateRangeTest {

    @Test
    public void testCreateFromMinMax_wrongUsage() throws Exception {
        try {
            DateRange.createFromMinMax(new ProductionRequest("test", "dummy"));
            fail();
        } catch (ProductionException pe) {
            assertEquals("Production parameter 'minDate' not set.", pe.getMessage());
        }
        try {
            DateRange.createFromMinMax(new ProductionRequest("test", "dummy", "minDate", "2001-02-03"));
            fail();
        } catch (ProductionException pe) {
            assertEquals("Production parameter 'maxDate' not set.", pe.getMessage());
        }
    }

    @Test
    public void testCreateFromMinMax() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest("test", "dummy", "minDate", "2001-02-03", "maxDate", "2002-04-06");
        DateRange dateRange = DateRange.createFromMinMax(productionRequest);
        assertNotNull(dateRange);
        assertNotNull(dateRange.getStartDate());
        assertNotNull(dateRange.getStopDate());
        assertEquals("2001-02-03", ProductionRequest.getDateFormat().format(dateRange.getStartDate()));
        assertEquals("2002-04-06", ProductionRequest.getDateFormat().format(dateRange.getStopDate()));
    }
}
