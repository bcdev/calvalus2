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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RecordWritableTest {

    public static final DateFormat DATE_FORMAT = SimpleDateFormat.getDateInstance(SimpleDateFormat.SHORT, Locale.ENGLISH);

    @Test
    public void testStringArgsConstructor() throws Exception {
        RecordWritable recordWritable = new RecordWritable(new String[] {"A", "B", "C"});
        String[] values = recordWritable.getValues();
        assertNotNull(values);
        assertEquals(3, values.length);
        assertEquals("A", values[0]);
        assertEquals("B", values[1]);
        assertEquals("C", values[2]);
    }

    @Test
    public void testObjectArgsConstructor() throws Exception {
        Date date = new Date(1313740506645L);
        RecordWritable recordWritable = new RecordWritable(new Object[] {'A', 76432, -2.14, date, true, "XYZ"},
                                                           DATE_FORMAT);
        String[] values = recordWritable.getValues();
        assertNotNull(values);
        assertEquals(6, values.length);
        assertEquals("A", values[0]);
        assertEquals("76432", values[1]);
        assertEquals("-2.14", values[2]);
        assertEquals("8/19/11", values[3]);
        assertEquals("true", values[4]);
        assertEquals("XYZ", values[5]);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        Date date = new Date(1313740506645L);
        RecordWritable recordWritable = new RecordWritable(new Object[] {'A', 76432, -2.14, date, true, "XYZ"},
                                                           DATE_FORMAT);

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
        assertEquals("76432", values[1]);
        assertEquals("-2.14", values[2]);
        assertEquals("8/19/11", values[3]);
        assertEquals("true", values[4]);
        assertEquals("XYZ", values[5]);
    }
}
