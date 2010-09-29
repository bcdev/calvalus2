package com.bc.calvados.hadoop.io;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ByteArrayWritableTest {
    @Test
    public void testNoArg() {
        ByteArrayWritable arrayWritable = new ByteArrayWritable();
        assertEquals(0, arrayWritable.getLength());
        assertNotNull(arrayWritable.getArray());
        assertEquals(0, arrayWritable.getArray().length);
    }

    @Test
    public void testArg() {
        ByteArrayWritable arrayWritable = new ByteArrayWritable(16);
        assertEquals(16, arrayWritable.getLength());
        assertNotNull(arrayWritable.getArray());
        assertEquals(16, arrayWritable.getArray().length);
    }

    @Test
    public void testIO() throws IOException {
        ByteArrayWritable writable1 = new ByteArrayWritable(5);
        assertEquals(5, writable1.getLength());
        writable1.getArray()[0] = (byte) 247;
        writable1.getArray()[1] = (byte) 3;
        writable1.getArray()[2] = (byte) 176;
        writable1.getArray()[3] = (byte) 89;
        writable1.getArray()[4] = (byte) -1;

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(32);
        writable1.write(new DataOutputStream(byteArrayOutputStream));
        byte[] buf = byteArrayOutputStream.toByteArray();

        ByteArrayWritable writable2 = new ByteArrayWritable();
        writable2.readFields(new DataInputStream(new ByteArrayInputStream(buf)));

        assertEquals(writable1.getLength(), writable2.getLength());
        assertEquals(writable1.getArray()[0], writable2.getArray()[0]);
        assertEquals(writable1.getArray()[1], writable2.getArray()[1]);
        assertEquals(writable1.getArray()[2], writable2.getArray()[2]);
        assertEquals(writable1.getArray()[3], writable2.getArray()[3]);
        assertEquals(writable1.getArray()[4], writable2.getArray()[4]);
    }
}
