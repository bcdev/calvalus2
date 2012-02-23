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


import org.junit.After;
import org.junit.Before;
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
 *
 * @author Norman Fomferra
 */
public class MappedByteBufferTest {

    interface FileIO {
        void write(File file, int n, Producer producer) throws IOException;

        int read(File file, Consumer consumer) throws IOException;
    }

    interface Producer {
        long createKey();

        float[] createSamples();
    }

    interface Consumer {
        void process(long key, float[] samples);
    }


    static final int MiB = 1024 * 1024;
    static final int N = 25000;

    File file;

    @Before
    public void setUp() throws Exception {
        file = getTestFile();
        file.deleteOnExit();
    }

    @After
    public void tearDown() throws Exception {
        file.delete();
    }

    @Test
    public void testThatMemoryMappedFileIODoesNotConsumeHeapSpace() throws Exception {
        final long mem1 = getFreeMiB();

        final int fileSize = Integer.MAX_VALUE; // 2GB!
        final FileChannel fc = new RandomAccessFile(file, "rw").getChannel();
        final MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        final long mem2 = getFreeMiB();
        // Modify buffer, so that it must be written when channel is closed.
        buffer.putDouble(1.2);
        buffer.putFloat(3.4f);
        buffer.putLong(fileSize - 8, 123456789L);
        final long mem3 = getFreeMiB();
        fc.close();
        final long mem4 = getFreeMiB();

        assertTrue(file.exists());
        assertEquals(fileSize, file.length());

        System.out.println("free mem before opening: " + mem1 + " MiB");
        System.out.println("free mem after opening:  " + mem2 + " MiB");
        System.out.println("free mem before closing: " + mem3 + " MiB");
        System.out.println("free mem after closing:  " + mem4 + " MiB");

        // If these memory checks fail, check if 1 MiB is still to fine grained
        assertEquals(mem2, mem1);
        assertEquals(mem3, mem1);
        assertEquals(mem4, mem1);

        // Now make sure we get the values back
        final DataInputStream stream = new DataInputStream(new FileInputStream(file));
        assertEquals(1.2, stream.readDouble(), 1e-10); // 8 bytes
        assertEquals(3.4, stream.readFloat(), 1e-5f);  // 4 bytes
        stream.skip(fileSize - (8 + 4 + 8));
        assertEquals(123456789L, stream.readLong());
        stream.close();
    }

    @Test
    public void testThatFileMappingsCanGrow() throws Exception {

        final int chunkSize = 100 * MiB;

        final FileChannel fc = new RandomAccessFile(file, "rw").getChannel();
        try {
            final MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, chunkSize);
            buffer.putDouble(0, 0.111);
            buffer.putDouble(chunkSize - 8, 1.222);
        } finally {
            fc.close();
            assertEquals(chunkSize, file.length());
        }

        final FileChannel fc2 = new RandomAccessFile(file, "rw").getChannel();
        try {
            final MappedByteBuffer buffer2 = fc2.map(FileChannel.MapMode.READ_WRITE, 0, 2 * chunkSize);
            assertEquals(0.111, buffer2.getDouble(0), 1e-10);
            assertEquals(1.222, buffer2.getDouble(chunkSize - 8), 1e-10);
            buffer2.putDouble(2 * chunkSize - 8, 2.333);
        } finally {
            fc2.close();
            assertEquals(2 * chunkSize, file.length());
        }

        final FileChannel fc3 = new RandomAccessFile(file, "rw").getChannel();
        try {
            final MappedByteBuffer buffer3 = fc3.map(FileChannel.MapMode.READ_WRITE, 0, 3 * chunkSize);
            assertEquals(0.111, buffer3.getDouble(0), 1e-10);
            assertEquals(1.222, buffer3.getDouble(chunkSize - 8), 1e-10);
            assertEquals(2.333, buffer3.getDouble(2 * chunkSize - 8), 1e-10);
            buffer3.putDouble(3 * chunkSize - 8, 3.444);
        } finally {
            fc3.close();
            assertEquals(3 * chunkSize, file.length());
        }

        final FileChannel fc4 = new RandomAccessFile(file, "rw").getChannel();
        try {
            final MappedByteBuffer buffer4 = fc4.map(FileChannel.MapMode.READ_WRITE, 0, 3 * chunkSize);
            assertEquals(0.111, buffer4.getDouble(0), 1e-10);
            assertEquals(1.222, buffer4.getDouble(chunkSize - 8), 1e-10);
            assertEquals(2.333, buffer4.getDouble(2 * chunkSize - 8), 1e-10);
            assertEquals(3.444, buffer4.getDouble(3 * chunkSize - 8), 1e-10);
        } finally {
            fc4.close();
            assertEquals(3 * chunkSize, file.length());
        }
    }

    @Test
    public void testStreamedFileIOPerformance() throws Exception {
        testFileIOPerformance(new StreamedFileIO());
    }

    @Test
    public void testMemoryMappedFileIOPerformance() throws Exception {
        testFileIOPerformance(new MemoryMappedFileIO());
    }

    private void testFileIOPerformance(FileIO fileIO) throws IOException {

        System.out.println("Testing " + fileIO.getClass().getSimpleName() + " for " + N + " samples");

        MyProducer producer = new MyProducer();
        MyConsumer consumer = new MyConsumer();

        long t1 = System.currentTimeMillis();
        fileIO.write(file, N, producer);
        long t2 = System.currentTimeMillis();
        fileIO.read(file, consumer);
        long t3 = System.currentTimeMillis();

        assertEquals(N, producer.n);
        assertEquals(N, consumer.n);

        System.out.println("buf write time: " + (t2 - t1) + " ms");
        System.out.println("buf read time:  " + (t3 - t2) + " ms");
        System.out.println("buf total time: " + (t3 - t1) + " ms");
        System.out.println("file size:      " + file.length() + " bytes");
    }

    class MemoryMappedFileIO implements FileIO {
        @Override
        public void write(File file, int n, Producer producer) throws IOException {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 100L * MiB);
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

        @Override
        public int read(File file, Consumer consumer) throws IOException {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            FileChannel channel = raf.getChannel();
            long length = file.length();
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, length);
            int n = 0;
            try {
                while (true) {
                    long key = readKey(buffer);
                    if (key == -1L) {
                        break;
                    }
                    float[] samples = readSamples(buffer);
                    consumer.process(key, samples);
                    n++;
                }
            } finally {
                channel.close();
                raf.close();
            }
            return n;
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


    }

    class StreamedFileIO implements FileIO {
        @Override
        public void write(File file, int n, Producer producer) throws IOException {
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

        @Override
        public int read(File file, Consumer consumer) throws IOException {
            int n = 0;
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
                    n++;
                }
            } finally {
                is.close();
            }
            return n;
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

    private static long getFreeMiB() {
        return Runtime.getRuntime().freeMemory() / MiB;
    }

    private File getTestFile() {
        return new File(MappedByteBufferTest.class.getSimpleName() + "$" + Long.toHexString(System.nanoTime()) + ".dat");
    }

}