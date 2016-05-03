package com.bc.calvalus.processing.fire;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FireGridReducerTest {

    @Test
    public void testCreateFilename() throws Exception {
        assertEquals("20080607-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", FireGridReducer.createFilename("2008", "06", true));
        assertEquals("20080622-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", FireGridReducer.createFilename("2008", "06", false));

        assertEquals("20101007-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", FireGridReducer.createFilename("2010", "10", true));
        assertEquals("20101022-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", FireGridReducer.createFilename("2010", "10", false));
    }

}