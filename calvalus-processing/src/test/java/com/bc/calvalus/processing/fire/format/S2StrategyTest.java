package com.bc.calvalus.processing.fire.format;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S2StrategyTest {

    private S2Strategy s2Strategy;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("sensor", "MERIS");
        s2Strategy = new S2Strategy(conf);
    }

    @Test
    public void testGetTile() throws Exception {
        String[] paths = {"dummy_T34NPN", "hdfs://calvalus/calvalus/projects/fire/aux/lc/2010.nc"};
        assertEquals("34NPN", s2Strategy.getTile(false, paths));
    }

    @Test
    public void testGetTile_hasBA() throws Exception {
        String[] paths = {"hdfs://calvalus/calvalus/projects/fire/s2-ba/BA-T32PKU-20151218T102004.nc", "hdfs://calvalus/calvalus/projects/fire/aux/lc/2010.nc"};
        assertEquals("32PKU", s2Strategy.getTile(true, paths));
    }

}