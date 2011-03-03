package com.bc.calvalus.binning;

import org.junit.Test;

import static org.junit.Assert.*;

public class BinManagerImplTest {
    @Test
    public void testBinCreation() {
        VariableContext vctx = new MyVariableContext("a", "b", "c");
        BinManager binManager = new BinManagerImpl(new AggregatorAverage(vctx, "c", null, null),
                                                   new AggregatorAverageML(vctx, "b", null, null),
                                                   new AggregatorMinMax(vctx, "a", null),
                                                   new AggregatorOnMaxSet(vctx, "c", "a", "b"));

        assertEquals(4, binManager.getAggregatorCount());

        SpatialBin sbin = binManager.createSpatialBin(42);
        assertEquals(42, sbin.getIndex());
        assertEquals(2 + 2 + 2 + 3, sbin.getPropertyCount());

        TemporalBin tbin = binManager.createTemporalBin(42);
        assertEquals(42, tbin.getIndex());
        assertEquals(3 + 3 + 2 + 3, tbin.getPropertyCount());
    }


}
