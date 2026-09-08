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


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link org.apache.hadoop.io.Writable} to hold a tile index.
 *
 * @author MarcoZ
 */
public class CubeIndexWritable implements WritableComparable {

    private short variableIndex = -1;
    private byte tileY = -1;
    private byte tileX = -1;
    private int timeIndex = -1;

    public CubeIndexWritable() {
    }

    public CubeIndexWritable(short variableIndex, byte tileY, byte tileX, int timeIndex) {
        this.variableIndex = variableIndex;
        this.tileY = tileY;
        this.tileX = tileX;
        this.timeIndex = timeIndex;
    }

    public int getVariableIndex() {
        return variableIndex;
    }

    public int getTileY() {
        return tileY;
    }

    public int getTileX() {
        return tileX;
    }

    public int getTimeIndex() {
        return timeIndex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(variableIndex);
        out.writeByte(tileY);
        out.writeByte(tileX);
        out.writeInt(timeIndex);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        variableIndex = in.readShort();
        tileY = in.readByte();
        tileX = in.readByte();
        timeIndex = in.readInt();
    }

    public int compareTo(Object o) {
        CubeIndexWritable that = (CubeIndexWritable) o;
        int result = compareInts(this.variableIndex, that.variableIndex);
        if (result == 0) {
            result = compareInts(this.tileY, that.tileY);
            if (result == 0) {
                result = compareInts(this.tileX, that.tileX);
                if (result == 0) {
                    result = compareInts(this.timeIndex, that.timeIndex);
                }
            }
        }
        return result;
    }

    private static int compareInts(int thisInt, int thatInt) {
        if (thisInt < thatInt) {
            return -1;
        } else if (thisInt == thatInt) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CubeIndexWritable)) {
            return false;
        }
        CubeIndexWritable other = (CubeIndexWritable) obj;
        return this.variableIndex == other.variableIndex &&
                this.tileY == other.tileY &&
                this.tileX == other.tileX &&
                this.timeIndex == other.timeIndex;
    }


    public int hashCode() {
        int hash = 31 + variableIndex;
        hash = (31 * hash) + tileY;
        hash = (31 * hash) + tileX;
        hash = (31 * hash) + timeIndex;
        return hash;
    }

    public String toString() {
        return new StringBuilder().
                append("(").append(variableIndex).append(",[").append(tileY).
                append(",").append(tileX).append("],").append(timeIndex).append(")").toString();
    }

    /**
     * A Comparator optimized for TileIndexWritable.
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(CubeIndexWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            int thisVariableIndex = readUnsignedShort(b1, s1);
            int thatVariableIndex = readUnsignedShort(b2, s2);
            int result = compareInts(thisVariableIndex, thatVariableIndex);
            if (result == 0) {
                int thisTileY = b1[s1 + 2];
                int thatTileY = b2[s2 + 2];
                result = compareInts(thisTileY, thatTileY);
                if (result == 0) {
                    int thisTileX = b1[s1 + 3];
                    int thatTileX = b2[s2 + 3];
                    result = compareInts(thisTileX, thatTileX);
                    if (result == 0) {
                        int thisTimeIndex = readInt(b1, s1 + 4);
                        int thatTimeIndex = readInt(b2, s2 + 4);
                        result = compareInts(thisTimeIndex, thatTimeIndex);
                    }
                }
            }
            return result;
        }
    }

    // register this comparator
    static {
        WritableComparator.define(CubeIndexWritable.class, new Comparator());
    }
}
