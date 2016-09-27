package com.bc.calvalus.processing.fire.format;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S2StrategyTest {

    @Test
    public void testGetTile() throws Exception {
        S2Strategy s2Strategy = new S2Strategy();
        String[] paths = {"dummy_T34NPN", "hdfs://calvalus/calvalus/projects/fire/aux/lc/2010.nc"};
        assertEquals("34NPN", s2Strategy.getTile(false, paths));
    }

    @Test
    public void testGetTile_hasBA() throws Exception {
        S2Strategy s2Strategy = new S2Strategy();
        String[] paths = {"hdfs://calvalus/calvalus/projects/fire/s2-ba/BA-T32PKU-20151218T102004.nc", "hdfs://calvalus/calvalus/projects/fire/aux/lc/2010.nc"};
        assertEquals("32PKU", s2Strategy.getTile(true, paths));
    }

}