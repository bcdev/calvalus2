package com.bc.calvalus.processing.ma;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import static org.junit.Assert.*;

public class RecordWritable2Test {

    public static final DateFormat DATE_FORMAT = SimpleDateFormat.getDateInstance(SimpleDateFormat.SHORT, Locale.ENGLISH);

    @Test
    public void testConstructor() throws Exception {
        RecordWritable2 recordWritable = new RecordWritable2();
        assertArrayEquals(null, recordWritable.getValues());
        assertEquals(null, recordWritable.getDateFormat());
        assertEquals('\t', recordWritable.getSeparatorChar());
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

        RecordWritable2 recordWritable = new RecordWritable2();
        recordWritable.setValues(inputValues);
        recordWritable.setDateFormat(DATE_FORMAT);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        recordWritable.write(new DataOutputStream(out));
        out.close();
        byte[] bytes = out.toByteArray();

        recordWritable = new RecordWritable2();
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

        RecordWritable2 recordWritable = new RecordWritable2();
        recordWritable.setValues(inputValues);
        recordWritable.setDateFormat(DATE_FORMAT);

        assertEquals("" +
                             "A\t" +
                             "76432\t" +
                             "-2.14\t" +
                             "8/19/11\t" +
                             "1\t" +
                             "0.5\t0.1\t15", recordWritable.toString());
    }
}
