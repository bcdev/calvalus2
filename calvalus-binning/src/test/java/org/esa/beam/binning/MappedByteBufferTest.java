/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package org.esa.beam.binning;


import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests runtime behaviour and performance of {@link FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
 * May be used to store intermediate spatial bins.
 */
public class MappedByteBufferTest {
    final long MB = 1024L * 1024L;
    final int N = 25000;

    @Test
    public void testMBB() throws Exception {

        System.out.println("free mem: " + Runtime.getRuntime().freeMemory() / MB + " MiB");

        FileChannel fc = new RandomAccessFile("test.dat", "rw").getChannel();
        MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
        System.out.println("free mem: " + Runtime.getRuntime().freeMemory() / MB + " MiB");
        buffer.putDouble(1.2);
        buffer.putFloat(3.4f);
        buffer.putLong(1000, 123456789L);
        fc.close();

        System.out.println("free mem: " + Runtime.getRuntime().freeMemory() / MB + " MiB");

        File file = new File("test.dat");
        assertTrue(file.exists());
        assertEquals(Integer.MAX_VALUE, file.length());

        DataInputStream stream = new DataInputStream(new FileInputStream(file));
        assertEquals(1.2, stream.readDouble(), 1e-10);
        assertEquals(3.4, stream.readFloat(), 1e-5f);
        assertEquals(0L, stream.readLong());
        stream.skip(1000 - (8 + 4 + 8));
        assertEquals(123456789L, stream.readLong());
        stream.close();

        System.out.println("size of " + file + ": " + file.length() + " bytes");

        file.deleteOnExit();
    }


    @Test
    public void testStreamIO() throws Exception {
        File file = new File("streamio.dat");
        MyProducer producer = new MyProducer();
        MyConsumer consumer = new MyConsumer();

        long t1 = System.currentTimeMillis();
        writeStreamed(file, N, producer);
        long t2 = System.currentTimeMillis();
        readStreamed(file, consumer);
        long t3 = System.currentTimeMillis();

        assertEquals(N, producer.n);
        assertEquals(N, consumer.n);

        System.out.println("write time: " + (t2 - t1) + "ms");
        System.out.println("read time:  " + (t3 - t2) + "ms");
        System.out.println("total time: " + (t3 - t1) + "ms");
        System.out.println("size of " + file + ": " + file.length() + " bytes");

        file.deleteOnExit();
    }

    @Test
    public void testBufferIO() throws Exception {
        File file = new File("bufferio.dat");
        MyProducer producer = new MyProducer();
        MyConsumer consumer = new MyConsumer();

        long t1 = System.currentTimeMillis();
        writeMapped(file, N, producer);
        long t2 = System.currentTimeMillis();
        readMapped(file, consumer);
        long t3 = System.currentTimeMillis();

        assertEquals(N, producer.n);
        assertEquals(N, consumer.n);

        System.out.println("buf write time: " + (t2 - t1) + " ms");
        System.out.println("buf read time:  " + (t3 - t2) + " ms");
        System.out.println("buf total time: " + (t3 - t1) + " ms");
        System.out.println("size of " + file + ": " + file.length() + " bytes");

        file.deleteOnExit();
    }

    private void writeStreamed(File file, int n, Producer producer) throws IOException {
        DataOutputStream os = new DataOutputStream(new FileOutputStream(file));
        try {
            for (int i = 0; i < n; i++) {
                long key = producer.createKey();
                float[] samples = producer.createSamples();
                writeKey(os, key);
                writeSamples(os, samples);
            }
        } finally {
            os.close();
        }
    }

    private void readStreamed(File file, Consumer consumer) throws IOException {
        DataInputStream is = new DataInputStream(new FileInputStream(file));
        try {
            while (true) {
                long key;
                try {
                    key = readKey(is);
                } catch (EOFException eof) {
                    break;
                }
                float[] samples = readSamples(is);
                consumer.process(key, samples);
            }
        } finally {
            is.close();
        }
    }

    private long readKey(DataInputStream is) throws IOException {
        return is.readLong();
    }

    private float[] readSamples(DataInputStream is) throws IOException {
        int n = is.readInt();
        float[] samples = new float[n];
        for (int i = 0; i < samples.length; i++) {
            samples[i] = is.readFloat();
        }
        return samples;
    }

    private void writeKey(DataOutputStream stream, long key) throws IOException {
        stream.writeLong(key);
    }

    private void writeSamples(DataOutputStream stream, float[] samples) throws IOException {
        stream.writeInt(samples.length);
        for (float sample : samples) {
            stream.writeFloat(sample);
        }
    }

    private void writeMapped(File file, int n, Producer producer) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel channel = raf.getChannel();
        ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 100 * MB);

        try {
            for (int i = 0; i < n; i++) {
                long key = producer.createKey();
                float[] samples = producer.createSamples();
                writeKey(buffer, key);
                writeSamples(buffer, samples);
            }
        } finally {
            writeKey(buffer, -1L);
            channel.close();
            raf.close();
        }
    }

    private void readMapped(File file, Consumer consumer) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel channel = raf.getChannel();
        long length = file.length();
        ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, length);
        try {
            while (true) {
                long key = readKey(buffer);
                if (key == -1L) {
                    break;
                }
                float[] samples = readSamples(buffer);
                consumer.process(key, samples);
            }
        } finally {
            channel.close();
            raf.close();
        }
    }

    private long readKey(ByteBuffer is) throws IOException {
        return is.getLong();
    }

    private float[] readSamples(ByteBuffer is) throws IOException {
        int n = is.getInt();
        float[] samples = new float[n];
        for (int i = 0; i < samples.length; i++) {
            samples[i] = is.getFloat();
        }
        return samples;
    }

    private void writeKey(ByteBuffer stream, long key) throws IOException {
        stream.putLong(key);
    }

    private void writeSamples(ByteBuffer stream, float[] samples) throws IOException {
        stream.putInt(samples.length);
        for (float sample : samples) {
            stream.putFloat(sample);
        }
    }


    /**
     * Sample producer which creates randomly-sized sample arrays (length: 0 ... 10).
     */
    private static class MyProducer implements Producer {
        long n;

        @Override
        public long createKey() {
            return n++;
        }

        @Override
        public float[] createSamples() {
            final float[] samples = new float[(int) (Math.random() * 11)];
            for (int i = 0; i < samples.length; i++) {
                samples[i] = 0.1f * i;
            }
            return samples;
        }
    }

    /**
     * Sample consumer which simply counts received sample arrays.
     */
    private static class MyConsumer implements Consumer {
        long n;

        @Override
        public void process(long key, float[] samples) {
            n++;
        }
    }

    interface Producer {
        long createKey();

        float[] createSamples();
    }

    interface Consumer {
        void process(long key, float[] samples);
    }

}
