package com.bc.calvalus.processing.ma;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Date;

import static org.junit.Assert.*;

public class RecordWritableTest {

    @Test
    public void testConstructor() throws Exception {
        RecordWritable recordWritable = new RecordWritable();
        assertArrayEquals(null, recordWritable.getValues());
    }

    @Test
    public void testWriteAndRead() throws Exception {
        Object[] inputValues = {
                "A",
                76432,
                -2.14,
                new Date(1313740506645L),
                1,
                new AggregatedNumber(15, 25, 5, 0.0, 1.0, 0.5, 0.1)
        };

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
    }

    @Test
    public void testToString() throws Exception {
        Object[] inputValues = {
                "A",
                76432,
                -2.14,
                new Date(1313740506645L),
                1,
                new AggregatedNumber(15, 25, 5, 0.0, 1.0, 0.5, 0.1)
        };

        RecordWritable recordWritable = new RecordWritable();
        recordWritable.setValues(inputValues);

        assertEquals("" +
                             "A\t" +
                             "76432\t" +
                             "-2.14\t" +
                             "2011-08-19 07:55:06\t" +
                             "1\t" +
                             "0.5\t0.1\t15", recordWritable.toString());
    }
}
