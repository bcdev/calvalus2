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

package com.bc.calvalus.processing.hadoop;

import com.bc.ceres.core.Assert;
import org.apache.hadoop.io.WritableComparator;

/**
 * Utility methods for hadoop writables
 */
public class WritableUtils {

    /**
     * Converts a byte array into a float array.
     * The array must match in size.
     */
    public static void convertByteToFloat(byte[] byteArray, float[] floatArray) {
        Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        for (int i = 0; i < floatArray.length; i++) {
            floatArray[i] = WritableComparator.readFloat(byteArray, i * 4);
        }
    }

    /**
     * Converts a float array into a byte array.
     * The array must match in size.
     */
    public static void convertFloatToByte(float[] floatArray, byte[] byteArray) {
        Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        int bi = 0;
        for (float aFloat : floatArray) {
            int intBits = Float.floatToIntBits(aFloat);
            byteArray[bi++] = (byte) ((intBits >>> 24) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 16) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 8) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 0) & 0xFF);
        }
    }

}
