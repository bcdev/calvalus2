package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModisFireGridDataSourceTest {


    @Test
    public void testGetUpperLat() throws Exception {
        assertEquals(90, AbstractFireGridDataSource.getUpperLat(0), 1E-5);
        assertEquals(-89.75, AbstractFireGridDataSource.getUpperLat(719), 1E-5);
        assertEquals(-0.25, AbstractFireGridDataSource.getUpperLat(361), 1E-5);
        assertEquals(11.5, AbstractFireGridDataSource.getUpperLat(314), 1E-5);
        assertEquals(64.75, AbstractFireGridDataSource.getUpperLat(101), 1E-5);
        try {
            AbstractFireGridDataSource.getUpperLat(-1);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
        try {
            AbstractFireGridDataSource.getUpperLat(720);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
    }

    @Test
    public void testGetLeftLon() throws Exception {
        assertEquals(-180, AbstractFireGridDataSource.getLeftLon(0), 1E-5);
        assertEquals(179.75, AbstractFireGridDataSource.getLeftLon(1439), 1E-5);
        assertEquals(0.0, AbstractFireGridDataSource.getLeftLon(720), 1E-5);
        assertEquals(-89.75, AbstractFireGridDataSource.getLeftLon(361), 1E-5);
        assertEquals(-101.5, AbstractFireGridDataSource.getLeftLon(314), 1E-5);
        assertEquals(175, AbstractFireGridDataSource.getLeftLon(1420), 1E-5);
        try {
            AbstractFireGridDataSource.getLeftLon(-1);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
        try {
            AbstractFireGridDataSource.getLeftLon(1440);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
    }

    @Test
    public void testGetLatAndLon() {
        assertEquals(-180.0, ModisFireGridDataSource.getLeftLon(0, "0,220"), 1E-5);
        assertEquals(-180.0, ModisFireGridDataSource.getLeftLon(0, "0,120"), 1E-5);
        assertEquals(-179.75, ModisFireGridDataSource.getLeftLon(1, "0,700"), 1E-5);
        assertEquals(-172.0, ModisFireGridDataSource.getLeftLon(0, "32,700"), 1E-5);
        assertEquals(172.0, ModisFireGridDataSource.getLeftLon(0, "1408,220"), 1E-5);
        assertEquals(179.75, ModisFireGridDataSource.getLeftLon(31, "1408,220"), 1E-5);

        assertEquals(90, ModisFireGridDataSource.getTopLat(0, "32,0"), 1E-5);
        assertEquals(89.75, ModisFireGridDataSource.getTopLat(1, "32,0"), 1E-5);
        assertEquals(82.25, ModisFireGridDataSource.getTopLat(31, "32,0"), 1E-5);
        assertEquals(-86, ModisFireGridDataSource.getTopLat(0, "320,704"), 1E-5);
        assertEquals(-89.75, ModisFireGridDataSource.getTopLat(15, "320,704"), 1E-5);
    }
}