package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.VectorImpl;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TAResultTest {

    @Test
    public void testReportGeneration() throws Exception {

        TAResult result = new TAResult();
        result.setOutputFeatureNames("chl_conc_mean", "chl_conc_stdev");
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
