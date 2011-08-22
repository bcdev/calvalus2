package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Norman
 */
public class RecordTransformerTest {

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksZeroLengthArrays() throws Exception {
        new RecordTransformer(-1).expand(ExtractorTest.newRecord(null, null,
                                                            "x",
                                                            new float[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksVaryingLengthArrays() throws Exception {
        new RecordTransformer(-1).expand(ExtractorTest.newRecord(null, null,
                                                            "x",
                                                            new float[3],
                                                            new int[2]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksSupportedArrayTypes() throws Exception {
        new RecordTransformer(-1).expand(ExtractorTest.newRecord(null, null,
                                                                 "x",
                                                                 new Date[3]));
    }

    @Test
    public void testExpand() throws Exception {
        List<Record> flattenedRecords = new RecordTransformer(-1).expand(ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                                                                                 "africa",
                                                                                                 new float[]{1.1F, 1.2F, 1.4F, 1.8F},
                                                                                                 new int[]{64, 32, 16, 8}));
        assertNotNull(flattenedRecords);
        assertEquals(4, flattenedRecords.size()  );

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
        List<Record> flattenedRecords = new RecordTransformer(6).expand(ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
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
        new RecordTransformer(-1).aggregate(ExtractorTest.newRecord(null, null,
                                                                 "x",
                                                                 new float[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksVaryingLengthArrays() throws Exception {
        new RecordTransformer(-1).aggregate(ExtractorTest.newRecord(null, null,
                                                                 "x",
                                                                 new float[3],
                                                                 new int[2]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksSupportedArrayTypes() throws Exception {
        new RecordTransformer(-1).aggregate(ExtractorTest.newRecord(null, null,
                                                                 "x",
                                                                 new Date[3]));
    }

    @Test
    public void testAggregate() throws Exception {
        Record aggregatedRecord = new RecordTransformer(-1).aggregate(ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                                                                           "africa",
                                                                                           new float[]{1.1F, 1.2F, 1.4F, 1.8F},
                                                                                           new int[]{64, 32, 16, 8},
                                                                                           new int[]{0, 1, 1, 0}));
        assertNotNull(aggregatedRecord);

        assertEquals(53.0F, (Float) aggregatedRecord.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) aggregatedRecord.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), aggregatedRecord.getAttributeValues()[2]);
        assertEquals("africa", aggregatedRecord.getAttributeValues()[3]);
        assertEquals((1.1F + 1.2F + 1.4F + 1.8F) / 4.0F, (Float) aggregatedRecord.getAttributeValues()[4], 1E-5F);
        assertEquals((64 + 32 + 16 + 8) / 4.0F, (Float) aggregatedRecord.getAttributeValues()[5], 1E-5F);
        assertEquals(0.5F, (Float) aggregatedRecord.getAttributeValues()[6], 1E-5F);
    }

/*
    @Test
    public void testAggregateWithMask() throws Exception {
        Record aggregatedRecord = new RecordTransformer(-1).aggregate(ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                                                                           "africa",
                                                                                           new float[]{1.1F, 1.2F, Float.NaN, 1.8F},
                                                                                           new int[]{64, 32, 16, 8},
                                                                                           new int[]{1, 1, 1, 0}));
        assertNotNull(aggregatedRecord);

        assertEquals(53.0F, (Float) aggregatedRecord.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) aggregatedRecord.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), aggregatedRecord.getAttributeValues()[2]);
        assertEquals("africa", aggregatedRecord.getAttributeValues()[3]);
        assertEquals((1.1F + 1.2F) / 2.0F, (Float) aggregatedRecord.getAttributeValues()[4], 1E-5F);
        assertEquals((64 + 32 + 16) / 3.0F, (Float) aggregatedRecord.getAttributeValues()[5], 1E-5F);
        assertEquals(3.0F / 4.0F, (Float) aggregatedRecord.getAttributeValues()[6], 1E-5F);
    }
*/
}
