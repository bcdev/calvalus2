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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A Hadoop writable for a cube chunk.
 *
 * @author MB
 */
public class CubeChunkWritable extends CompressedWritable {

    private Object elems;
    private int length;

    public CubeChunkWritable() {
    }

    public CubeChunkWritable(Object elems, int length) {
        this.elems = elems;
        this.length = length;
    }

    public Object getSamples() {
        ensureInflated();
        return elems;
    }

    @Override
    public void writeCompressed(DataOutput out) throws IOException {
        if (elems instanceof float[]) {
            out.writeByte(ProductData.TYPE_FLOAT32);
            out.writeInt(length);
            byte[] buffer = new byte[length * 4];
            WritableUtils.convertFloatToByte((float[])elems, buffer);
            out.write(buffer);
        } else if (elems instanceof int[]) {
            out.writeByte(ProductData.TYPE_INT32);
            out.writeInt(length);
            byte[] buffer = new byte[length * 4];
            WritableUtils.convertIntToByte((int[])elems, buffer);
            out.write(buffer);
        } else if (elems instanceof short[]) {
            out.writeByte(ProductData.TYPE_INT16);
            out.writeInt(length);
            byte[] buffer = new byte[length * 2];
            WritableUtils.convertShortToByte((short[])elems, buffer);
            out.write(buffer);
        } else if (elems instanceof byte[]) {
            out.writeByte(ProductData.TYPE_INT8);
            out.writeInt(length);
            byte[] buffer = new byte[length];
            System.arraycopy((byte[])elems, 0, buffer, 0, length);
            out.write(buffer);
        } else if (elems instanceof double[]) {
            out.writeByte(ProductData.TYPE_FLOAT64);
            out.writeInt(length);
            byte[] buffer = new byte[length * 8];
            WritableUtils.convertDoubleToByte((double[])elems, buffer);
            out.write(buffer);
        } else if (elems instanceof long[]) {
            out.writeByte(ProductData.TYPE_INT64);
            out.writeInt(length);
            byte[] buffer = new byte[length * 8];
            WritableUtils.convertLongToByte((long[])elems, buffer);
            out.write(buffer);
        } else {
            throw new IllegalArgumentException("unknown type of " + elems);
        }
    }

    @Override
    public void readFieldsCompressed(DataInput in) throws IOException {
        final int elemType = in.readByte();
        final int numElems = in.readInt();
        final byte[] byteBuffer;
        switch (elemType) {
            case ProductData.TYPE_FLOAT32:
                float[] floats = (float[]) this.elems;
                if (floats == null || floats.length < numElems) {
                    floats = new float[numElems];
                    this.elems = floats;
                }
                byteBuffer = new byte[numElems * 4];
                in.readFully(byteBuffer);
                WritableUtils.convertByteToFloat(byteBuffer, floats);
                break;
            case ProductData.TYPE_INT32:
                int[] ints = (int[]) this.elems;
                if (ints == null || ints.length < numElems) {
                    ints = new int[numElems];
                    this.elems = ints;
                }
                byteBuffer = new byte[numElems * 4];
                in.readFully(byteBuffer);
                WritableUtils.convertByteToInt(byteBuffer, ints);
                break;
            case ProductData.TYPE_INT16:
                short[] shorts = (short[]) this.elems;
                if (shorts == null || shorts.length < numElems) {
                    shorts = new short[numElems];
                    this.elems = shorts;
                }
                byteBuffer = new byte[numElems * 2];
                in.readFully(byteBuffer);
                WritableUtils.convertByteToShort(byteBuffer, shorts);
                break;
            case ProductData.TYPE_INT8:
                byte[] bytes = (byte[]) this.elems;
                if (bytes == null || bytes.length < numElems) {
                    bytes = new byte[numElems];
                    this.elems = bytes;
                }
                in.readFully(bytes, 0, numElems);
                break;
            case ProductData.TYPE_FLOAT64:
                double[] doubles = (double[]) this.elems;
                if (doubles == null || doubles.length < numElems) {
                    doubles = new double[numElems];
                    this.elems = doubles;
                }
                byteBuffer = new byte[numElems * 8];
                in.readFully(byteBuffer);
                WritableUtils.convertByteToDouble(byteBuffer, doubles);
                break;
            case ProductData.TYPE_INT64:
                long[] longs = (long[]) this.elems;
                if (longs == null || longs.length < numElems) {
                    longs = new long[numElems];
                    this.elems = longs;
                }
                byteBuffer = new byte[numElems * 8];
                in.readFully(byteBuffer);
                WritableUtils.convertByteToLong(byteBuffer, longs);
                break;
            default:
                throw new IllegalArgumentException("unknown chunk encoding type " + elemType);
        }
    }
}



