package com.bc.calvalus.processing.fire.format.pixel.s2;

import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.support.VectorImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JDAggregatorTest {

    private JDAggregator aggregator;

    @Before
    public void setUp() throws Exception {
        // use absurd min/max doy for testing; see tests testAggregate_5 ff for serious tests of this property
        aggregator = new JDAggregator(null, null, null, null, new int[]{-1000, 1000});
    }

    @Test
    public void testAggregate_1() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        aggregator.aggregate(997F, 997F, ctx, targetVector);
        aggregator.aggregate(300F, 0.5F, ctx, targetVector);
        aggregator.aggregate(998F, 998F, ctx, targetVector);

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

        aggregator.aggregate(998F, 998F, ctx, targetVector);
        aggregator.aggregate(999F, 999F, ctx, targetVector);
        aggregator.aggregate(997F, 997F, ctx, targetVector);

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

        aggregator.aggregate(10F, 0, ctx, targetVector);
        aggregator.aggregate(20F, 0, ctx, targetVector);
        aggregator.aggregate(999F, 0, ctx, targetVector);
        aggregator.aggregate(997F, 0, ctx, targetVector);

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

        aggregator.aggregate(999F, 0, ctx, targetVector);
        aggregator.aggregate(998F, 0, ctx, targetVector);
        aggregator.aggregate(997F, 0, ctx, targetVector);

        assertEquals(997F, targetVector.get(0), 1E-7);
        assertEquals(0, targetVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_5() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);
        JDAggregator aggregator = new JDAggregator(null, null, null, null, new int[]{0, 31});

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        aggregator.aggregate(5F, 0, ctx, targetVector);
        aggregator.aggregate(10F, 0, ctx, targetVector);
        aggregator.aggregate(997F, 0, ctx, targetVector);
        aggregator.aggregate(100F, 0, ctx, targetVector);
        aggregator.aggregate(45F, 0, ctx, targetVector);

        assertEquals(10F, targetVector.get(0), 1E-7);
        assertEquals(0, targetVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_6() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);
        JDAggregator aggregator = new JDAggregator(null, null, null, null, new int[]{32, 60});

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        aggregator.aggregate(5F, 0, ctx, targetVector);
        aggregator.aggregate(10F, 0, ctx, targetVector);
        aggregator.aggregate(997F, 0, ctx, targetVector);
        aggregator.aggregate(100F, 0, ctx, targetVector);
        aggregator.aggregate(45F, 0, ctx, targetVector);

        assertEquals(45F, targetVector.get(0), 1E-7);
        assertEquals(0, targetVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_7() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);
        JDAggregator aggregator = new JDAggregator(null, null, null, null, new int[]{32, 60});

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        aggregator.aggregate(998F, 0, ctx, targetVector);
        aggregator.aggregate(33F, 0, ctx, targetVector);
        aggregator.aggregate(61F, 0, ctx, targetVector);
        aggregator.aggregate(320F, 0, ctx, targetVector);
        aggregator.aggregate(997F, 0, ctx, targetVector);

        assertEquals(33F, targetVector.get(0), 1E-7);
        assertEquals(0, targetVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_8() throws Exception {
        SpatialBin ctx = new SpatialBin();
        VectorImpl targetVector = new VectorImpl(new float[2]);
        JDAggregator aggregator = new JDAggregator(null, null, null, null, new int[]{32, 60});

        targetVector.set(0, 0.0f);
        targetVector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);

        aggregator.aggregate(220F, 0, ctx, targetVector);

        assertEquals(0F, targetVector.get(0), 1E-7);
        assertEquals(0, targetVector.get(1), 1E-7);
    }



}