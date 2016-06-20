package com.bc.calvalus.processing.fire.format.grid;

import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class FireGridReducerTest {

    @Test
    public void testCreateFilename() throws Exception {
        assertEquals("20080607-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", GridReducer.createFilename("2008", "06", true));
        assertEquals("20080622-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", GridReducer.createFilename("2008", "06", false));

        assertEquals("20101007-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", GridReducer.createFilename("2010", "10", true));
        assertEquals("20101022-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", GridReducer.createFilename("2010", "10", false));
    }

    @Test
    public void testCreateTimeString() throws Exception {
        String localTimeString = GridReducer.createTimeString(Instant.parse("2007-12-03T10:15:30.00Z"));
        assertEquals("2007-12-03 11:15:30", localTimeString);
    }
}