package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.processing.hadoop.ByteArrayWritable;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class ByteArrayWritableTest {
    @Test
    public void testNoArgConstructor() {
        ByteArrayWritable arrayWritable = new ByteArrayWritable();
        Assert.assertEquals(0, arrayWritable.getLength());
        assertNotNull(arrayWritable.getArray());
        Assert.assertEquals(0, arrayWritable.getArray().length);
    }

    @Test
    public void testOneArgConstructor() {
        ByteArrayWritable arrayWritable = new ByteArrayWritable(16);
        Assert.assertEquals(16, arrayWritable.getLength());
        assertNotNull(arrayWritable.getArray());
        Assert.assertEquals(16, arrayWritable.getArray().length);
    }

    @Test
    public void testInputOutput() throws IOException {
        ByteArrayWritable writable1 = new ByteArrayWritable(5);
        Assert.assertEquals(5, writable1.getLength());
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

        Assert.assertEquals(writable1.getLength(), writable2.getLength());
        Assert.assertEquals(writable1.getArray()[0], writable2.getArray()[0]);
        Assert.assertEquals(writable1.getArray()[1], writable2.getArray()[1]);
        Assert.assertEquals(writable1.getArray()[2], writable2.getArray()[2]);
        Assert.assertEquals(writable1.getArray()[3], writable2.getArray()[3]);
        Assert.assertEquals(writable1.getArray()[4], writable2.getArray()[4]);
    }
}
