package com.bc.calvalus.processing.fire.format.grid.s2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by Thomas on 28.02.2018.
 */
public class S2GridReducerTest {

    @Test
    public void testGetY() throws Exception {
        assertEquals(272, new S2GridReducer().getY("2016-08-x210y112"));
        assertEquals(368, new S2GridReducer().getY("2016-08-x210y88"));
        assertEquals(376, new S2GridReducer().getY("2016-08-x210y86"));
    }

}