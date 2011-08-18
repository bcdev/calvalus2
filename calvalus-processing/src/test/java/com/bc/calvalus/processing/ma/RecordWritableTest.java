package com.bc.calvalus.processing.ma;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.*;

public class RecordWritableTest {
    @Test
    public void testConstructorDoesTheJob() throws Exception {
        RecordWritable recordWritable = new RecordWritable(DefaultRecord.create(null, "A", "B", "C"),
                                                           DefaultRecord.create(null, 1, 2, false));
        String[] values = recordWritable.getValues();
        assertNotNull(values);
        assertEquals(6, values.length);
        assertEquals("A", values[0]);
        assertEquals("B", values[1]);
        assertEquals("C", values[2]);
        assertEquals("1", values[3]);
        assertEquals("2", values[4]);
        assertEquals("false", values[5]);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        RecordWritable recordWritable = new RecordWritable(DefaultRecord.create(null, "A", "B", "C"),
                                                           DefaultRecord.create(null, 1, 2, false));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        recordWritable.write(new DataOutputStream(out));
        out.close();
        byte[] bytes = out.toByteArray();

        recordWritable = new RecordWritable();
        recordWritable.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

        String[] values = recordWritable.getValues();
        assertNotNull(values);
        assertEquals(6, values.length);
        assertEquals("A", values[0]);
        assertEquals("B", values[1]);
        assertEquals("C", values[2]);
        assertEquals("1", values[3]);
        assertEquals("2", values[4]);
        assertEquals("false", values[5]);
    }
}
