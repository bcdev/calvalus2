package com.bc.calvalus.processing.fire.format.grid.s2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by Thomas on 28.02.2018.
 */
public class S2GridReducerTest {

    @Test
    public void testGetX() throws Exception {
        assertEquals(840, new S2GridReducer().getX("2016-08-x210y112"));
        assertEquals(848, new S2GridReducer().getX("2016-08-x212y88"));
        assertEquals(360, new S2GridReducer().getX("2016-08-x90y86"));
    }

    @Test
    public void testGetY() throws Exception {
        assertEquals(272, new S2GridReducer().getY("2016-08-x210y112"));
        assertEquals(368, new S2GridReducer().getY("2016-08-x210y88"));
        assertEquals(376, new S2GridReducer().getY("2016-08-x210y86"));
    }

}