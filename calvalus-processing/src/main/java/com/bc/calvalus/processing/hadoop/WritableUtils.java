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
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        final int stop = Math.min(floatArray.length, byteArray.length / 4);
        for (int i = 0; i < stop; i++) {
            floatArray[i] = WritableComparator.readFloat(byteArray, i * 4);
        }
    }

    public static void convertByteToInt(byte[] byteArray, int[] floatArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        final int stop = Math.min(floatArray.length, byteArray.length / 4);
        for (int i = 0; i < stop; i++) {
            floatArray[i] = WritableComparator.readInt(byteArray, i * 4);
        }
    }

    public static void convertByteToShort(byte[] byteArray, short[] floatArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        final int stop = Math.min(floatArray.length, byteArray.length / 2);
        for (int i = 0; i < stop; i++) {
            floatArray[i] = (short) WritableComparator.readUnsignedShort(byteArray, i * 2);
        }
    }

    public static void convertByteToDouble(byte[] byteArray, double[] floatArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        final int stop = Math.min(floatArray.length, byteArray.length / 8);
        for (int i = 0; i < stop; i++) {
            floatArray[i] = WritableComparator.readDouble(byteArray, i * 8);
        }
    }

    public static void convertByteToLong(byte[] byteArray, long[] floatArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        final int stop = Math.min(floatArray.length, byteArray.length / 8);
        for (int i = 0; i < stop; i++) {
            floatArray[i] = WritableComparator.readLong(byteArray, i * 8);
        }
    }

    /**
     * Converts a float array into a byte array.
     * The array must match in size.
     */
    public static void convertFloatToByte(float[] floatArray, byte[] byteArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        int bi = 0;
        for (float aFloat : floatArray) {
            int intBits = Float.floatToIntBits(aFloat);
            byteArray[bi++] = (byte) ((intBits >>> 24) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 16) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 8) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 0) & 0xFF);
            if (bi >= byteArray.length) {
                break;
            }
        }
    }

    /**
     * Converts a float array into a byte array.
     * The array must match in size.
     */
    public static void convertIntToByte(int[] floatArray, byte[] byteArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        int bi = 0;
        for (int intBits : floatArray) {
            byteArray[bi++] = (byte) ((intBits >>> 24) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 16) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 8) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 0) & 0xFF);
            if (bi >= byteArray.length) {
                break;
            }
        }
    }

    /**
     * Converts a float array into a byte array.
     * The array must match in size.
     */
    public static void convertShortToByte(short[] floatArray, byte[] byteArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        int bi = 0;
        for (short intBits : floatArray) {
            byteArray[bi++] = (byte) ((intBits >>> 8) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 0) & 0xFF);
            if (bi >= byteArray.length) {
                break;
            }
        }
    }

    /**
     * Converts a float array into a byte array.
     * The array must match in size.
     */
    public static void convertDoubleToByte(double[] floatArray, byte[] byteArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        int bi = 0;
        for (double aFloat : floatArray) {
            long intBits = Double.doubleToLongBits(aFloat);
            byteArray[bi++] = (byte) ((intBits >>> 56) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 48) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 40) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 32) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 24) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 16) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 8) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 0) & 0xFF);
            if (bi >= byteArray.length) {
                break;
            }
        }
    }

    /**
     * Converts a float array into a byte array.
     * The array must match in size.
     */
    public static void convertLongToByte(long[] floatArray, byte[] byteArray) {
        //Assert.argument(4 * floatArray.length == byteArray.length, "4 * floatArray.length == byteArray.length");
        int bi = 0;
        for (long intBits : floatArray) {
            byteArray[bi++] = (byte) ((intBits >>> 56) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 48) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 40) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 32) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 24) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 16) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 8) & 0xFF);
            byteArray[bi++] = (byte) ((intBits >>> 0) & 0xFF);
            if (bi >= byteArray.length) {
                break;
            }
        }
    }

}
