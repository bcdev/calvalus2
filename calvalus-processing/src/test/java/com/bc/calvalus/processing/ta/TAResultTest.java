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

package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.VectorImpl;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TAResultTest {

    @Test
    public void testReportGeneration() throws Exception {

        TAResult result = new TAResult(1);
        result.setOutputFeatureNames(0, new String[]{"chl_conc_mean", "chl_conc_stdev"});
        result.addRecord("balticsea", "2008-06-04", "2008-06-06", new VectorImpl(new float[] {0.3F, 0.1F}));
        result.addRecord("northsea", "2008-06-07", "2008-06-09", new VectorImpl(new float[] {0.8F, 0.2F}));
        result.addRecord("balticsea", "2008-06-07", "2008-06-09", new VectorImpl(new float[] {0.1F, 0.2F}));
        result.addRecord("northsea", "2008-06-04", "2008-06-06", new VectorImpl(new float[] {0.4F, 0.0F}));
        result.addRecord("balticsea", "2008-06-01", "2008-06-03", new VectorImpl(new float[] {0.8F, 0.0F}));
        result.addRecord("northsea", "2008-06-01", "2008-06-03", new VectorImpl(new float[] {0.3F, 0.1F}));
        result.addRecord("northsea", "2008-06-13", "2008-06-15", new VectorImpl(new float[] {0.4F, 0.4F}));
        result.addRecord("balticsea", "2008-06-10", "2008-06-12", new VectorImpl(new float[] {0.6F, 0.3F}));
        result.addRecord("northsea", "2008-06-10", "2008-06-12", new VectorImpl(new float[] {0.2F, 0.1F}));

        Set<String> regionNames = result.getRegionNames();
        assertNotNull(regionNames);
        assertEquals(2, regionNames.size());
        assertTrue(regionNames.contains("northsea"));
        assertTrue(regionNames.contains("balticsea"));

        List<TAResult.Record> northsea = result.getRecords("northsea");
        assertNotNull(northsea);
        assertEquals(5, northsea.size());
        assertEquals("2008-06-01", northsea.get(0).startDate);
        assertEquals("2008-06-04", northsea.get(1).startDate);
        assertEquals("2008-06-07", northsea.get(2).startDate);
        assertEquals("2008-06-10", northsea.get(3).startDate);
        assertEquals("2008-06-13", northsea.get(4).startDate);

        List<TAResult.Record> balticsea = result.getRecords("balticsea");
        assertNotNull(balticsea);
        assertEquals(4, balticsea.size());
        assertEquals("2008-06-01", balticsea.get(0).startDate);
        assertEquals("2008-06-04", balticsea.get(1).startDate);
        assertEquals("2008-06-07", balticsea.get(2).startDate);
        assertEquals("2008-06-10", balticsea.get(3).startDate);

        List<TAResult.Record> atlantic = result.getRecords("atlantic");
        assertNull(atlantic);
    }
}
