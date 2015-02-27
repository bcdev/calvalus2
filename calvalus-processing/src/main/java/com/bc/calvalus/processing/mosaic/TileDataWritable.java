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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.processing.hadoop.WritableUtils;
import org.apache.hadoop.io.CompressedWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A Hadoop writable for a bunch of {@link org.esa.beam.framework.gpf.Tile}s.
 *
 * @author MarcoZ
 */
public class TileDataWritable extends CompressedWritable {

    // an array of databuffers
    private float[][] sampleValues;


    public TileDataWritable() {
    }

    public TileDataWritable(float[][] sampleValues) {
        this.sampleValues = sampleValues;
    }

    public float[][] getSamples() {
        ensureInflated();
        return sampleValues;
    }

    @Override
    public void writeCompressed(DataOutput out) throws IOException {
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
        int numBands = in.readInt();
        int numElems = in.readInt();
        float[][] array2D = this.sampleValues;
        if (array2D == null || array2D.length != numBands || array2D[0].length != numElems) {
            array2D = new float[numBands][numElems];
        }
        byte[] byteBuffer = new byte[numElems * 4];
        for (float[] array1D : array2D) {
            in.readFully(byteBuffer);
            WritableUtils.convertByteToFloat(byteBuffer, array1D);
        }
        this.sampleValues = array2D;
    }

    public String toString() {
        if (sampleValues != null && sampleValues.length > 0 && sampleValues[0] != null) {
            return "TileDataWritable(" + sampleValues.length + "," + sampleValues[0].length + ")";
        } else {
            return "TileDataWritable(null)";
        }
    }
}



