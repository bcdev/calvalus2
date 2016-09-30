package com.bc.calvalus.processing.fire.format;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MerisStrategyTest {

    private MerisStrategy strategy;

    @Before
    public void setUp() throws Exception {
        strategy = new MerisStrategy();
    }

    @Test
    public void testComputeTargetWidth() throws Exception {
        assertEquals(45720, strategy.computeTargetWidth(getArea("ASIA")));
    }

    @Test
    public void testComputeTargetHeightWidth() throws Exception {
        assertEquals(29880, strategy.computeTargetHeight(getArea("ASIA")));
    }

    @Test
    public void testGetLeftTargetXForTile() throws Exception {
        assertEquals(6120, strategy.getLeftTargetXForTile(getArea("ASIA"), "2006-08-v04h25"));
        assertEquals(0, strategy.getLeftTargetXForTile(getArea("ASIA"), "2006-08-v04h23"));
        assertEquals(2520, strategy.getLeftTargetXForTile(getArea("ASIA"), "2006-08-v04h24"));

        assertEquals(9360, strategy.getLeftTargetXForTile(getArea("EUROPE"), "2006-08-v04h18"));
        assertEquals(0, strategy.getLeftTargetXForTile(getArea("EUROPE"), "2006-08-v04h15"));
        assertEquals(2160, strategy.getLeftTargetXForTile(getArea("EUROPE"), "2006-08-v04h16"));

        assertEquals(0, strategy.getLeftTargetXForTile(getArea("NORTH_AMERICA"), "2006-08-v04h00"));
    }

    @Test
    public void testGetTopTargetYForTile() throws Exception {
        assertEquals(11880, strategy.getTopTargetYForTile(getArea("ASIA"), "2006-08-v04h25"));
        assertEquals(0, strategy.getTopTargetYForTile(getArea("ASIA"), "2006-08-v00h25"));
        assertEquals(1080, strategy.getTopTargetYForTile(getArea("ASIA"), "2006-08-v01h25"));

        assertEquals(0, strategy.getTopTargetYForTile(getArea("AUSTRALIA"), "2006-08-v09h34"));
    }

    @Test
    public void testGetLeftSourceX() throws Exception {
        assertEquals(1080, strategy.getLeftSourceXForTile(getArea("ASIA"), "2008-06-v02h23"));
        assertEquals(0, strategy.getLeftSourceXForTile(getArea("ASIA"), "2008-06-v02h24"));
        assertEquals(0, strategy.getLeftSourceXForTile(getArea("ASIA"), "2008-06-v02h25"));
    }

    private PixelProductArea getArea(String id) {
        return strategy.getArea(id);
    }

}