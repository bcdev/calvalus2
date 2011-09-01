package com.bc.calvalus.processing.ma;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Date;

import static org.junit.Assert.*;

public class RecordWritableTest {

    private Object[] inputValues;

    @Before
    public void setUp() throws Exception {
        inputValues = new Object[]{
                "Benguela",
                76432,
                -2.14,
                new Date(1313740506645L),
                Double.NaN,
                1,
                new AggregatedNumber(15, 25, 5, 0.0, 1.0, 0.5, 0.1),
                new AggregatedNumber(4, 9, 1, 0.0, 1.0, 3.4, 0.2, new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9})
        };
    }

    @Test
    public void testConstructor() throws Exception {
        RecordWritable recordWritable = new RecordWritable();
        assertArrayEquals(null, recordWritable.getValues());
    }

    @Test
    public void testWriteAndRead() throws Exception {

        RecordWritable recordWritable = new RecordWritable(inputValues);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        recordWritable.write(new DataOutputStream(out));
        out.close();
        byte[] bytes = out.toByteArray();

        recordWritable = new RecordWritable();
        recordWritable.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

        Object[] outputValues = recordWritable.getValues();
        assertNotNull(outputValues);
        assertArrayEquals(inputValues, outputValues);

        assertEquals(AggregatedNumber.class, outputValues[6].getClass());
        AggregatedNumber num7 = (AggregatedNumber) outputValues[6];
        assertNull(num7.data);

        assertEquals(AggregatedNumber.class, outputValues[7].getClass());
        AggregatedNumber num8 = (AggregatedNumber) outputValues[7];
        assertNotNull(num8.data);
        assertEquals(9, num8.data.length);
        assertEquals(1.0, num8.data[0], 1e-6F);
        assertEquals(9.0, num8.data[8], 1e-6F);
    }

    @Test
    public void testToString() throws Exception {

        RecordWritable recordWritable = new RecordWritable();
        recordWritable.setValues(inputValues);

        assertEquals("" +
                             "Benguela\t" +
                             "76432\t" +
                             "-2.14\t" +
                             "2011-08-19 07:55:06\t" +
                             "NaN\t" +
                             "1\t" +
                             "0.5\t" +
                             "3.4",
                     recordWritable.toString());
    }
}
