/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra;

import com.bc.calvalus.processing.hadoop.WritableUtils;
import org.apache.hadoop.io.CompressedWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link org.apache.hadoop.io.Writable} to hold a region analysis extract.
 *
 * @author MarcoZ
 */
public class RAValue extends CompressedWritable {

    private int numObs;
    private float[][] sampleValues;
    private long time;
    private String productName;

    // required by Hadoop
    public RAValue() {
    }

    public RAValue(int numObs, float[][] samples, long time, String productName) {
        this.numObs = numObs;
        this.sampleValues = samples;
        this.time = time;
        this.productName = productName;
    }

    public int getNumObs() {
        ensureInflated();
        return numObs;
    }

    public String getProductName() {
        ensureInflated();
        return productName;
    }

    public long getTime() {
        ensureInflated();
        return time;
    }

    public float[][] getSamples() {
        ensureInflated();
        return sampleValues;
    }

    @Override
    public void writeCompressed(DataOutput out) throws IOException {
        out.writeInt(numObs);
        out.writeLong(time);
        Text.writeString(out, productName);
        float[][] array2D = sampleValues;
        int numBands = array2D.length;
        int numElems = array2D[0].length;
        out.writeInt(numBands);
        out.writeInt(numElems);
        byte[] byteBuffer = new byte[numElems * 4];
        for (float[] array1D : array2D) {
            WritableUtils.convertFloatToByte(array1D, byteBuffer);
            out.write(byteBuffer);
        }
    }

    @Override
    public void readFieldsCompressed(DataInput in) throws IOException {
        numObs = in.readInt();
        time = in.readLong();
        productName = Text.readString(in);
        int numBands = in.readInt();
        int numSamples = in.readInt();
        float[][] array2D = sampleValues;
        if (array2D == null || array2D.length != numBands || array2D[0].length != numSamples) {
            array2D = new float[numBands][numSamples];
        }
        byte[] byteBuffer = new byte[numSamples * 4];
        for (float[] array1D : array2D) {
            in.readFully(byteBuffer);
            WritableUtils.convertByteToFloat(byteBuffer, array1D);
        }
        sampleValues = array2D;
    }

    public String toString() {
        if (sampleValues != null && sampleValues.length > 0 && sampleValues[0] != null) {
            return "ExtractWritable(" + sampleValues.length + "," + sampleValues[0].length + ")";
        } else {
            return "ExtractWritable(null)";
        }
    }
}
