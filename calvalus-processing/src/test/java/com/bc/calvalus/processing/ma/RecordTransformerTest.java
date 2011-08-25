package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class RecordTransformerTest {

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksZeroLengthArrays() throws Exception {
        new RecordTransformer(-1, 1.5).expand(ProductRecordSourceTest.newRecord(null, null,
                                                                 "x",
                                                                 new float[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksVaryingLengthArrays() throws Exception {
        new RecordTransformer(-1, 1.5).expand(ProductRecordSourceTest.newRecord(null, null,
                                                                 "x",
                                                                 new float[3],
                                                                 new int[2]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksSupportedArrayTypes() throws Exception {
        new RecordTransformer(-1, 1.5).expand(ProductRecordSourceTest.newRecord(null, null,
                                                                 "x",
                                                                 new Date[3]));
    }

    @Test
    public void testExpand() throws Exception {
        List<Record> flattenedRecords = new RecordTransformer(-1, 1.5).expand(ProductRecordSourceTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                                                                                 "africa",
                                                                                                 new float[]{1.1F, 1.2F, 1.4F, 1.8F},
                                                                                                 new int[]{64, 32, 16, 8}));
        assertNotNull(flattenedRecords);
        assertEquals(4, flattenedRecords.size());

        Record r1 = flattenedRecords.get(0);
        assertEquals(6, r1.getAttributeValues().length);
        assertEquals(53.0F, (Float) r1.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) r1.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), r1.getAttributeValues()[2]);
        assertEquals("africa", r1.getAttributeValues()[3]);
        assertEquals(1.1F, (Float) r1.getAttributeValues()[4], 1E-5F);
        assertEquals(64, r1.getAttributeValues()[5]);

        Record r4 = flattenedRecords.get(3);
        assertEquals(6, r4.getAttributeValues().length);
        assertEquals(53.0F, (Float) r4.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) r4.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), r4.getAttributeValues()[2]);
        assertEquals("africa", r4.getAttributeValues()[3]);
        assertEquals(1.8F, (Float) r4.getAttributeValues()[4], 1E-5F);
        assertEquals(8, r4.getAttributeValues()[5]);
    }

    @Test
    public void testExpandWithMask() throws Exception {
        List<Record> flattenedRecords = new RecordTransformer(6, 1.5).expand(ProductRecordSourceTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                                                                                "africa",
                                                                                                new float[]{1.1F, 1.2F, 1.4F, 1.8F},
                                                                                                new int[]{64, 32, 16, 8},
                                                                                                new int[]{1, 0, 0, 1}));
        assertNotNull(flattenedRecords);
        assertEquals(2, flattenedRecords.size());

        Record r1 = flattenedRecords.get(0);
        assertEquals(7, r1.getAttributeValues().length);
        assertEquals(53.0F, (Float) r1.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) r1.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), r1.getAttributeValues()[2]);
        assertEquals("africa", r1.getAttributeValues()[3]);
        assertEquals(1.1F, (Float) r1.getAttributeValues()[4], 1E-5F);
        assertEquals(64, r1.getAttributeValues()[5]);

        Record r4 = flattenedRecords.get(1);
        assertEquals(7, r4.getAttributeValues().length);
        assertEquals(53.0F, (Float) r4.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) r4.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), r4.getAttributeValues()[2]);
        assertEquals("africa", r4.getAttributeValues()[3]);
        assertEquals(1.8F, (Float) r4.getAttributeValues()[4], 1E-5F);
        assertEquals(8, r4.getAttributeValues()[5]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksZeroLengthArrays() throws Exception {
        new RecordTransformer(-1, 1.5).aggregate(ProductRecordSourceTest.newRecord(null, null,
                                                                    "x",
                                                                    new float[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksVaryingLengthArrays() throws Exception {
        new RecordTransformer(-1, 1.5).aggregate(ProductRecordSourceTest.newRecord(null, null,
                                                                    "x",
                                                                    new float[3],
                                                                    new int[2]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksSupportedArrayTypes() throws Exception {
        new RecordTransformer(-1, 1.5).aggregate(ProductRecordSourceTest.newRecord(null, null,
                                                                    "x",
                                                                    new Date[3]));
    }

    @Test
    public void testAggregate() throws Exception {
        int maskAttributeIndex = -1;
        Record aggregatedRecord = new RecordTransformer(maskAttributeIndex, 1.5).aggregate(
                ProductRecordSourceTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                        "africa",
                                        new float[]{0.2F, 1.2F, 1.2F, 1.4F, 1.8F},
                                        new int[]{13, 10, 11, 32, 14},
                                        new int[]{1, 0, 1, 1, 0}));
        assertNotNull(aggregatedRecord);

        assertEquals(53.0F, (Float) aggregatedRecord.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) aggregatedRecord.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), aggregatedRecord.getAttributeValues()[2]);
        assertEquals("africa", aggregatedRecord.getAttributeValues()[3]);

        Object v5 = aggregatedRecord.getAttributeValues()[4];
        assertEquals(AggregatedNumber.class, v5.getClass());
        AggregatedNumber a5 = (AggregatedNumber) v5;
        assertEquals(5, a5.numTotalPixels);
        assertEquals(5, a5.numGoodPixels);
        assertEquals(4, a5.numFilteredPixels);
        assertEquals(0.2, a5.min, 1E-6);
        assertEquals(1.8, a5.max, 1E-6);
        assertEquals(1.16, a5.mean, 1E-6);
        assertEquals(0.589915, a5.stdDev, 1E-6);
        assertEquals(1.4, a5.filteredMean, 1E-6);
        assertEquals(0.28284266, a5.filteredStdDev, 1E-6);
        assertEquals(a5.mean, a5.doubleValue(), 1E-6);

        Object v6 = aggregatedRecord.getAttributeValues()[5];
        assertEquals(AggregatedNumber.class, v6.getClass());
        AggregatedNumber a6 = (AggregatedNumber) v6;
        assertEquals(5, a6.numTotalPixels);
        assertEquals(5, a6.numGoodPixels);
        assertEquals(4, a6.numFilteredPixels);
        assertEquals(10.0, a6.min, 1E-6);
        assertEquals(32.0, a6.max, 1E-6);
        assertEquals(16.0, a6.mean, 1E-6);
        assertEquals(9.082951, a6.stdDev, 1E-6);
        assertEquals(12.0, a6.filteredMean, 1E-6);
        assertEquals(1.825742, a6.filteredStdDev, 1E-6);
        assertEquals(a6.mean, a6.doubleValue(), 1E-6);

        Object v7 = aggregatedRecord.getAttributeValues()[6];
        assertEquals(AggregatedNumber.class, v7.getClass());
        AggregatedNumber a7 = (AggregatedNumber) v7;
        assertEquals(5, a7.numGoodPixels);
        assertEquals(5, a7.numTotalPixels);
        assertEquals(0.0, a7.min, 1E-6);
        assertEquals(1.0, a7.max, 1E-6);
        assertEquals(0.6, a7.mean, 1E-6);
        assertEquals(a7.mean, a7.doubleValue(), 1E-6);
    }

    @Test
    public void testAggregateWithMask() throws Exception {
        int maskAttributeIndex = 6;
        Record aggregatedRecord = new RecordTransformer(maskAttributeIndex, 1.5).aggregate(
                ProductRecordSourceTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                        "africa",
                                        new float[]{1.1F, 1.2F, Float.NaN, 1.8F},
                                        new int[]{64, 32, 16, 8},
                                        new int[]{1, 1, 1, 0}));
        assertNotNull(aggregatedRecord);

        assertEquals(53.0F, (Float) aggregatedRecord.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) aggregatedRecord.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), aggregatedRecord.getAttributeValues()[2]);
        assertEquals("africa", aggregatedRecord.getAttributeValues()[3]);

        Object v5 = aggregatedRecord.getAttributeValues()[4];
        assertEquals(AggregatedNumber.class, v5.getClass());
        AggregatedNumber a5 = (AggregatedNumber) v5;
        assertEquals(2, a5.numGoodPixels);
        assertEquals(3, a5.numTotalPixels);
        assertEquals((1.1F + 1.2F) / 2, a5.floatValue(), 1E-5F);

        Object v6 = aggregatedRecord.getAttributeValues()[5];
        assertEquals(AggregatedNumber.class, v6.getClass());
        AggregatedNumber a6 = (AggregatedNumber) v6;
        assertEquals(3, a6.numGoodPixels);
        assertEquals(4, a6.numTotalPixels);
        assertEquals((64F + 32F + 16F) / 3, a6.floatValue(), 1E-5F);

        Object v7 = aggregatedRecord.getAttributeValues()[6];
        assertEquals(AggregatedNumber.class, v7.getClass());
        AggregatedNumber a7 = (AggregatedNumber) v7;
        assertEquals(4, a7.numGoodPixels);
        assertEquals(4, a7.numTotalPixels);
        assertEquals(3F / 4, a7.floatValue(), 1E-5F);
    }

    @Test
    public void testAggregateWithFilters() throws Exception {
        int maskAttributeIndex = -1;
        RecordFilter filter = new RecordFilter() {
            @Override
            public boolean accept(Record record) {
                return ((AggregatedNumber)record.getAttributeValues()[4]).mean < 1.0F;
            }
        };
        Record goodRecord = new RecordTransformer(maskAttributeIndex, 1.5, filter).aggregate(
                ProductRecordSourceTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                        "africa",
                                        new float[]{0.5F, 0.5F, 0.5F})); // mean = 0.5
        assertNotNull(goodRecord);

        Record badRecord = new RecordTransformer(maskAttributeIndex, 1.5, filter).aggregate(
                ProductRecordSourceTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                        "africa",
                                        new float[]{2.0F, 2.0F, 2.0F})); // mean = 2.0
        assertNull(badRecord);
    }


}
