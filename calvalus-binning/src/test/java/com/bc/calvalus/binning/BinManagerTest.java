package com.bc.calvalus.binning;

import com.bc.calvalus.binning.aggregators.AggregatorAverage;
import com.bc.calvalus.binning.aggregators.AggregatorAverageML;
import com.bc.calvalus.binning.aggregators.AggregatorMinMax;
import com.bc.calvalus.binning.aggregators.AggregatorOnMaxSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BinManagerTest {
    @Test
    public void testBinCreation() {
        VariableContext vctx = new MyVariableContext("a", "b", "c");
        BinManager binManager = new BinManager(new AggregatorAverage(vctx, "c", null, null),
                                               new AggregatorAverageML(vctx, "b", null, null),
                                               new AggregatorMinMax(vctx, "a", null),
                                               new AggregatorOnMaxSet(vctx, "c", "a", "b"));

        assertEquals(4, binManager.getAggregatorCount());

        SpatialBin sbin = binManager.createSpatialBin(42);
        assertEquals(42, sbin.getIndex());
        assertEquals(2 + 2 + 2 + 3, sbin.getFeatureValues().length);

        TemporalBin tbin = binManager.createTemporalBin(42);
        assertEquals(42, tbin.getIndex());
        assertEquals(3 + 3 + 2 + 3, tbin.getFeatureValues().length);
    }


}
