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
public class TileIndexWritable implements WritableComparable {

    private int macroTileX = -1;
    private int macroTileY = -1;
    private int tileX = -1;
    private int tileY = -1;

    public TileIndexWritable() {
    }

    public TileIndexWritable(int macroTileX, int macroTileY, int tileX, int tileY) {
        this.macroTileX = macroTileX;
        this.macroTileY = macroTileY;
        this.tileX = tileX;
        this.tileY = tileY;
    }

    public int getMacroTileX() {
        return macroTileX;
    }

    public int getMacroTileY() {
        return macroTileY;
    }

    public int getTileX() {
        return tileX;
    }

    public int getTileY() {
        return tileY;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(macroTileY);
        out.writeInt(macroTileX);
        out.writeInt(tileY);
        out.writeInt(tileX);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        macroTileY = in.readInt();
        macroTileX = in.readInt();
        tileY = in.readInt();
        tileX = in.readInt();
    }

    public int compareTo(Object o) {
        TileIndexWritable that = (TileIndexWritable) o;
        int result = compareInts(this.macroTileY, that.macroTileY);
        if (result == 0) {
            result = compareInts(this.macroTileX, that.macroTileX);
            if (result == 0) {
                result = compareInts(this.tileY, that.tileY);
                if (result == 0) {
                    result = compareInts(this.tileX, that.tileX);
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
        if (!(obj instanceof TileIndexWritable)) {
            return false;
        }
        TileIndexWritable other = (TileIndexWritable) obj;
        return this.tileX == other.tileX &&
                this.tileY == other.tileY &&
                this.macroTileX == other.macroTileX &&
                this.macroTileY == other.macroTileY;
    }


    public int hashCode() {
        int hash = 31 + macroTileX;
        hash = (31 * hash) + macroTileY;
        hash = (31 * hash) + tileX;
        hash = (31 * hash) + tileY;
        return hash;
    }

    public String toString() {
        return new StringBuilder().
                append("[").append(macroTileX).append(",").append(macroTileY).append("]").
                append("(").append(tileX).append(",").append(tileY).append(")").toString();
    }

    /**
     * A Comparator optimized for TileIndexWritable.
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(TileIndexWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            int thisMacroTileY = readInt(b1, s1);
            int thatMacroTileY = readInt(b2, s2);
            int result = compareInts(thisMacroTileY, thatMacroTileY);
            if (result == 0) {
                int thisMacroTileX = readInt(b1, s1 + 4);
                int thatMacroTileX = readInt(b2, s2 + 4);
                result = compareInts(thisMacroTileX, thatMacroTileX);
                if (result == 0) {
                    int thisTileY = readInt(b1, s1 + 8);
                    int thatTileY = readInt(b2, s2 + 8);
                    result = compareInts(thisTileY, thatTileY);
                    if (result == 0) {
                        int thisTileX = readInt(b1, s1 + 12);
                        int thatTileX = readInt(b2, s2 + 12);
                        result = compareInts(thisTileX, thatTileX);
                    }
                }
            }
            return result;
        }
    }

    // register this comparator
    static {
        WritableComparator.define(TileIndexWritable.class, new Comparator());
    }
}
