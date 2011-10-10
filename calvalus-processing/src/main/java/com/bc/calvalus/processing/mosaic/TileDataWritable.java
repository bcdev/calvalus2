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

    private int width;
    private int height;
    // an array of databuffers
    private float[][] sampleValues;


    public TileDataWritable() {
    }

    public TileDataWritable(int width, int height, float[][] sampleValues) {
        this.width = width;
        this.height = height;
        this.sampleValues = sampleValues;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public float[][] getSamples() {
        ensureInflated();
        return sampleValues;
    }

    @Override
    public void writeCompressed(DataOutput out) throws IOException {
        out.writeInt(width);
        out.writeInt(height);
        out.writeInt(sampleValues.length);
        for (float[] sampleValue : sampleValues) {
            for (float aSampleValue : sampleValue) {
                out.writeFloat(aSampleValue);
            }
        }
    }

    @Override
    public void readFieldsCompressed(DataInput in) throws IOException {
        width = in.readInt();
        height = in.readInt();
        int numBands = in.readInt();
        float[][] array2D = this.sampleValues;
        if (array2D == null || array2D.length != numBands || array2D[0].length != width * height) {
            array2D = new float[numBands][width * height];
        }
        for (float[] array1D : array2D) {
            for (int j = 0; j < array1D.length; j++) {
                array1D[j] = in.readFloat();
            }
        }
        this.sampleValues = array2D;
    }
}



