/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package org.esa.beam.binning.operator;

import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.SpatialBin;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
* @author Thomas Storm
*/
class MemoryMappedFileSpatialBinStore implements BinningOp.SpatialBinStore {

    private final File file;
    private final ByteBuffer consumeBuffer;
    private final RandomAccessFile consumeRaf;

    private static final int MB = 1024 * 1024;

    MemoryMappedFileSpatialBinStore() throws IOException {
        file = File.createTempFile(getClass().getSimpleName() + "-", ".dat");
        file.deleteOnExit();
        consumeRaf = new RandomAccessFile(file, "rw");
        FileChannel channel = consumeRaf.getChannel();
        consumeBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 100L * MB);
    }

    @Override
    public SortedMap<Long, List<SpatialBin>> getSpatialBinMap() throws IOException {
        final TreeMap<Long, List<SpatialBin>> spatialBinMap = new TreeMap<Long, List<SpatialBin>>();
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel channel = raf.getChannel();
        long length = file.length();
        ByteBuffer readBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, length);
        final DataInput dataInput = new ByteBufferWrapper(readBuffer);
        try {
            while (true) {
                long key = readKey(readBuffer);
                if (key == -1L) {
                    break;
                }
                if(!spatialBinMap.containsKey(key)) {
                    spatialBinMap.put(key, new ArrayList<SpatialBin>());
                }
                final SpatialBin spatialBin = SpatialBin.read(key, dataInput);
                spatialBinMap.get(key).add(spatialBin);
            }
        } finally {
            raf.close();
        }
        return spatialBinMap;
    }


    @Override
    public void consumeSpatialBins(BinningContext ignored, List<SpatialBin> spatialBins) throws IOException {
        for (SpatialBin spatialBin : spatialBins) {
            long key = spatialBin.getIndex();
            float[] samples = spatialBin.getFeatureValues();
            writeKey(consumeBuffer, key);
            consumeBuffer.putInt(spatialBin.getNumObs());
            consumeBuffer.putInt(spatialBin.getFeatureValues().length);
            writeSamples(consumeBuffer, samples);
        }
    }

    @Override
    public void consumingCompleted() throws IOException {
        writeKey(consumeBuffer, -1L);
        if(consumeRaf != null) {
            consumeRaf.close();
        }
        if (file.exists() && !file.delete()) {
            // todo - replace by system logging
            System.out.println("WARNING: Failed to delete temporal file '" + file.getAbsolutePath() + "'.");
        }
    }

    private long readKey(ByteBuffer buffer) throws IOException {
        return buffer.getLong();
    }

    private void writeKey(ByteBuffer buffer, long key) throws IOException {
        buffer.putLong(key);
    }

    private void writeSamples(ByteBuffer buffer, float[] samples) throws IOException {
        for (float sample : samples) {
            buffer.putFloat(sample);
        }
    }

    private static class ByteBufferWrapper extends ObjectInputStream {

        private final ByteBuffer buffer;

        public ByteBufferWrapper(ByteBuffer buffer) throws IOException {
            this.buffer = buffer;
        }

        @Override
        public int readInt() throws IOException {
            return buffer.getInt();
        }

        @Override
        public float readFloat() throws IOException {
            return buffer.getFloat();
        }
    }
}
