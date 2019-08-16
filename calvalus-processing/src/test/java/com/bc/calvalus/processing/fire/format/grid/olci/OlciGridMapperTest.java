package com.bc.calvalus.processing.fire.format.grid.olci;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OlciGridMapperTest {

    @Test
    public void testGetTile() {
        assertEquals("h31v10", OlciGridMapper.getTile("hdfs://calvalus/calvalus/projects/fire/auxiliary/lc-v2.1.1-split-for-olci/lc-h31v10.nc"));
    }

}