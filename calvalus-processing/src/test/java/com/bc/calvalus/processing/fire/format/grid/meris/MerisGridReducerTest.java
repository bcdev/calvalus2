package com.bc.calvalus.processing.fire.format.grid.meris;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MerisGridReducerTest {

    private MerisGridReducer reducer;

    @Before
    public void setUp() throws Exception {
        reducer = new MerisGridReducer();

    }

    @Test
    public void testCreateFilename() throws Exception {
        assertEquals("20080601-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", reducer.getFilename("2008", "06", "v04.0"));
        assertEquals("20101001-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc", reducer.getFilename("2010", "10", "v04.1"));
    }

}