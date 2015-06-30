package com.bc.calvalus.processing.ma;

import org.esa.snap.framework.datamodel.GeoPos;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class RecordAggregatorTest {

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksZeroLengthArrays() throws Exception {
        new RecordAggregator(-1, 1.5).transform(RecordUtils.create(null, null,
                                                                   "x",
                                                                   new float[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksVaryingLengthArrays() throws Exception {
        new RecordAggregator(-1, 1.5).transform(RecordUtils.create(null, null,
                                                                   "x",
                                                                   new float[3],
                                                                   new int[2]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksSupportedArrayTypes() throws Exception {
        new RecordAggregator(-1, 1.5).transform(RecordUtils.create(null, null,
                                                                   "x",
                                                                   new Date[3]));
    }

    @Test
    public void testAggregate() throws Exception {
        int maskAttributeIndex = -1;
        Record aggregatedRecord = new RecordAggregator(maskAttributeIndex, 1.5).transform(
                createDataRecord(new float[]{0.2F, 1.2F, 1.2F, 1.4F, 1.8F},
                                 new int[]{13, 10, 11, 32, 14},
                                 new int[]{1, 0, 1, 1, 0}));
        assertNotNull(aggregatedRecord);

        testFirst4Values(aggregatedRecord);

        Object v5 = aggregatedRecord.getAttributeValues()[4];
        assertEquals(AggregatedNumber.class, v5.getClass());
        AggregatedNumber a5 = (AggregatedNumber) v5;
        assertEquals(5, a5.nT);
        assertEquals(4, a5.n);
        assertEquals(1, a5.nF);
        assertEquals(1.2, a5.min, 1E-6);
        assertEquals(1.8, a5.max, 1E-6);
        assertEquals(1.4, a5.mean, 1E-6);
        assertEquals(0.28284266, a5.sigma, 1E-6);
        assertEquals(a5.mean, a5.doubleValue(), 1E-6);
        assertArrayEquals(new float[]{0.2F, 1.2F, 1.2F, 1.4F, 1.8F}, a5.data, 1E-6F);

        Object v6 = aggregatedRecord.getAttributeValues()[5];
        assertEquals(AggregatedNumber.class, v6.getClass());
        AggregatedNumber a6 = (AggregatedNumber) v6;
        assertEquals(5, a6.nT);
        assertEquals(4, a6.n);
        assertEquals(1, a6.nF);
        assertEquals(10.0, a6.min, 1E-6);
        assertEquals(14.0, a6.max, 1E-6);
        assertEquals(12.0, a6.mean, 1E-6);
        assertEquals(1.825742, a6.sigma, 1E-6);
        assertEquals(a6.mean, a6.doubleValue(), 1E-6);
        assertArrayEquals(new float[]{13, 10, 11, 32, 14}, a6.data, 1E-6F);

        Object v7 = aggregatedRecord.getAttributeValues()[6];
        assertEquals(AggregatedNumber.class, v7.getClass());
        AggregatedNumber a7 = (AggregatedNumber) v7;
        assertEquals(5, a7.n);
        assertEquals(5, a7.nT);
        assertEquals(0.0, a7.min, 1E-6);
        assertEquals(1.0, a7.max, 1E-6);
        assertEquals(0.6, a7.mean, 1E-6);
        assertEquals(a7.mean, a7.doubleValue(), 1E-6);
        assertArrayEquals(new float[]{1, 0, 1, 1, 0}, a7.data, 1E-6F);
    }

    private void testFirst4Values(Record aggregatedRecord) {
        assertEquals(53.0, (double)aggregatedRecord.getAttributeValues()[0], 1E-5);
        assertEquals(13.3, (double)aggregatedRecord.getAttributeValues()[1], 1E-5);
        assertEquals(new Date(128L), aggregatedRecord.getAttributeValues()[2]);
        assertEquals("africa", aggregatedRecord.getAttributeValues()[3]);
    }

    @Test
    public void testAggregateWithMask() throws Exception {
        int maskAttributeIndex = 6;
        Record aggregatedRecord = new RecordAggregator(maskAttributeIndex, 1.5).transform(
                createDataRecord(new float[]{1.1F, 1.2F, Float.NaN, 1.8F},
                                 new int[]{64, 32, 16, 8},
                                 new int[]{1, 1, 1, 0})); // = mask
        assertNotNull(aggregatedRecord);

        testFirst4Values(aggregatedRecord);

        Object v5 = aggregatedRecord.getAttributeValues()[4];
        assertEquals(AggregatedNumber.class, v5.getClass());
        AggregatedNumber a5 = (AggregatedNumber) v5;
        assertEquals(2, a5.n);
        assertEquals(3, a5.nT);
        assertEquals(0, a5.nF);
        assertEquals((1.1F + 1.2F) / 2, a5.floatValue(), 1E-5F);

        Object v6 = aggregatedRecord.getAttributeValues()[5];
        assertEquals(AggregatedNumber.class, v6.getClass());
        AggregatedNumber a6 = (AggregatedNumber) v6;
        assertEquals(3, a6.n);
        assertEquals(4, a6.nT);
        assertEquals(0, a6.nF);
        assertEquals((64F + 32F + 16F) / 3, a6.floatValue(), 1E-5F);

        // Test that the mask itself is treated differently: the mask shall not be applied to itself
        Object v7 = aggregatedRecord.getAttributeValues()[6];
        assertEquals(AggregatedNumber.class, v7.getClass());
        AggregatedNumber a7 = (AggregatedNumber) v7;
        assertEquals(4, a7.n);
        assertEquals(4, a7.nT);
        assertEquals(0, a7.nF);
        assertEquals((1F + 1F + 1F) / 4, a7.floatValue(), 1E-5F);
    }

    @Test
    public void testOneElementAggregationAllValid() throws Exception {
        int maskAttributeIndex = 6;
        Record aggregatedRecord = new RecordAggregator(maskAttributeIndex, 1.5).transform(
                createDataRecord(new float[]{1.1F},
                                 new int[]{64},
                                 new int[]{1}));  // = mask
        assertNotNull(aggregatedRecord);

        testFirst4Values(aggregatedRecord);

        Object v5 = aggregatedRecord.getAttributeValues()[4];
        assertEquals(Float.class, v5.getClass());
        Float a5 = (Float) v5;
        assertEquals(1.1F, a5, 1E-5F);

        Object v6 = aggregatedRecord.getAttributeValues()[5];
        assertEquals(Integer.class, v6.getClass());
        assertEquals(64, v6);

        Object v7 = aggregatedRecord.getAttributeValues()[6];
        assertEquals(Integer.class, v7.getClass());
        assertEquals(1, v7);
    }

    @Test
    public void testOneElementAggregationAllInvalid() throws Exception {
        int maskAttributeIndex = 6;
        float[] value5 = {1.1F};
        int[] value6 = {64};
        int[] mask = {0};
        Record aggregatedRecord = new RecordAggregator(maskAttributeIndex, 1.5).transform(
                createDataRecord(value5, value6, mask));  // = mask (INVALID now)
        assertNotNull(aggregatedRecord);

        testFirst4Values(aggregatedRecord);

        Object v5 = aggregatedRecord.getAttributeValues()[4];
        assertEquals(Float.class, v5.getClass());
        Float a5 = (Float) v5;
        assertTrue(Float.isNaN(a5));

        Object v6 = aggregatedRecord.getAttributeValues()[5];
        assertEquals(null, v6);

        Object v7 = aggregatedRecord.getAttributeValues()[6];
        assertEquals(Integer.class, v7.getClass());
        assertEquals(0, v7);
    }

    private Record createDataRecord(float[] value5, int[] value6, int[] mask) {
        return RecordUtils.create(new GeoPos(53.0F, 13.3F), new Date(128L),
                                  "africa",
                                  value5,
                                  value6,
                                  mask);
    }

}
