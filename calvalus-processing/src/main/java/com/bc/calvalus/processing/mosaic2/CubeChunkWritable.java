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

package com.bc.calvalus.processing.mosaic2;

import com.bc.calvalus.processing.hadoop.WritableUtils;
import org.apache.hadoop.io.CompressedWritable;
import org.esa.snap.core.datamodel.ProductData;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * A Hadoop writable for a cube chunk.
 *
 * @author MB
 */
public class CubeChunkWritable extends CompressedWritable {

    private byte typeLength;
    private byte[] fillBytes;
    private int length;
    private byte[] buffer;
    private Object elems;
    private ByteOrder byteOrder;
    private double fillValue;

    public CubeChunkWritable() {
    }

    public CubeChunkWritable(Object elems, int length, ByteOrder byteOrder, double fillValue) {
        this.elems = elems;
        this.length = length;
        this.byteOrder = byteOrder;
        this.fillValue = fillValue;
    }

    public Object getSamples() {
        ensureInflated();
        return buffer;
    }

    public byte[] getBuffer() {
        ensureInflated();
        return buffer;
    }

    public int getLength() {
        ensureInflated();
        return length;
    }

    public byte getTypeLength() {
        ensureInflated();
        return typeLength;
    }

    public byte[] getFillBytes() {
        ensureInflated();
        return fillBytes;
    }

    class PlainByteArrayOutputStream extends ByteArrayOutputStream {
        public PlainByteArrayOutputStream(int size) {
            super(size);
        }
        public byte[] toByteArray() {
            return buf;
        }
    }

    @Override
    public void writeCompressed(DataOutput out) throws IOException {
        if (elems instanceof float[]) {
            final ByteArrayOutputStream byteArrayStream = new PlainByteArrayOutputStream(4 + length * 4);
            final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(byteArrayStream);
            byteStream.setByteOrder(byteOrder);
            byteStream.writeFloat((float) fillValue);
            byteStream.writeFloats((float[]) elems, 0, length);
            out.writeByte(4);
            out.writeInt(length * 4);
            out.write(byteArrayStream.toByteArray());
        } else if (elems instanceof int[]) {
            final ByteArrayOutputStream byteArrayStream = new PlainByteArrayOutputStream(4 + length * 4);
            final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(byteArrayStream);
            byteStream.setByteOrder(byteOrder);
            byteStream.writeInt(Double.isFinite(fillValue) ? (int) fillValue : 0);
            byteStream.writeInts((int[]) elems, 0, length);
            out.writeByte(4);
            out.writeInt(length * 4);
            out.write(byteArrayStream.toByteArray());
        } else if (elems instanceof short[]) {
            final ByteArrayOutputStream byteArrayStream = new PlainByteArrayOutputStream(2 + length * 2);
            final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(byteArrayStream);
            byteStream.setByteOrder(byteOrder);
            byteStream.writeShort(Double.isFinite(fillValue) ? (short) fillValue : 0);
            byteStream.writeShorts((short[]) elems, 0, length);
            out.writeByte(2);
            out.writeInt(length * 2);
            out.write(byteArrayStream.toByteArray());
        } else if (elems instanceof byte[]) {
            final byte[] buffer = (byte[]) elems;
            out.writeByte(1);
            out.writeInt(length);
            out.write(Double.isFinite(fillValue) ? (byte) fillValue : (byte)0);
            out.write(buffer, 0, length);
        } else if (elems instanceof double[]) {
            final ByteArrayOutputStream byteArrayStream = new PlainByteArrayOutputStream(8 + length * 8);
            final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(byteArrayStream);
            byteStream.setByteOrder(byteOrder);
            byteStream.writeDouble(fillValue);
            byteStream.writeDoubles((double[]) elems, 0, length);
            final byte[] buffer = byteArrayStream.toByteArray();
            out.writeByte(8);
            out.writeInt(length * 8);
            out.write(buffer);
        } else if (elems instanceof long[]) {
            final ByteArrayOutputStream byteArrayStream = new PlainByteArrayOutputStream(8 + length * 8);
            final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(byteArrayStream);
            byteStream.setByteOrder(byteOrder);
            byteStream.writeLong(Double.isFinite(fillValue) ? (long)fillValue : 0L);
            byteStream.writeLongs((long[]) elems, 0, length);
            final byte[] buffer = byteArrayStream.toByteArray();
            out.writeByte(8);
            out.writeInt(length * 8);
            out.write(buffer);
        } else {
            throw new IllegalArgumentException("unexpected data type " + elems);
        }
    }

    @Override
    public void readFieldsCompressed(DataInput in) throws IOException {
        typeLength = in.readByte();
        length = in.readInt();
        fillBytes = new byte[typeLength];
        in.readFully(fillBytes);
        buffer = new byte[length];
        in.readFully(buffer);
    }
}



