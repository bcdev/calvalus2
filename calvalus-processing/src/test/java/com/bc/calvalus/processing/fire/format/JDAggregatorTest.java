package com.bc.calvalus.processing.fire.format;

import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.support.VectorImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JDAggregatorTest {

    @Test
    public void testAggregate_1() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        JDAggregator.aggregate(997F, 997F, ctx, targetVector);
        JDAggregator.aggregate(300F, 0.5F, ctx, targetVector);
        JDAggregator.aggregate(998F, 998F, ctx, targetVector);

        assertEquals(300F, targetVector.get(0), 1E-7);
        assertEquals(0.5F, targetVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_2() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        JDAggregator.aggregate(998F, 998F, ctx, targetVector);
        JDAggregator.aggregate(999F, 999F, ctx, targetVector);
        JDAggregator.aggregate(997F, 997F, ctx, targetVector);

        assertEquals(997F, targetVector.get(0), 1E-7);
        assertEquals(997F, targetVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_3() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        JDAggregator.aggregate(10F, 0, ctx, targetVector);
        JDAggregator.aggregate(20F, 0, ctx, targetVector);
        JDAggregator.aggregate(999F, 0, ctx, targetVector);
        JDAggregator.aggregate(997F, 0, ctx, targetVector);

        assertEquals(20F, targetVector.get(0), 1E-7);
        assertEquals(0, targetVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_4() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        JDAggregator.aggregate(999F, 0, ctx, targetVector);
        JDAggregator.aggregate(998F, 0, ctx, targetVector);
        JDAggregator.aggregate(997F, 0, ctx, targetVector);

        assertEquals(997F, targetVector.get(0), 1E-7);
        assertEquals(0, targetVector.get(1), 1E-7);
    }

}