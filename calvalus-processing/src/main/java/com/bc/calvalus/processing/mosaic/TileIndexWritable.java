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

    private int tileX;

    private int tileY;

    public TileIndexWritable() {
    }

    public TileIndexWritable(int tileX, int tileY) {
        this.tileX = tileX;
        this.tileY = tileY;
    }

    public int getTileX() {
        return tileX;
    }

    public int getTileY() {
        return tileY;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tileY);
        out.writeInt(tileX);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tileY = in.readInt();
        tileX = in.readInt();
    }

    public int compareTo(Object o) {
        TileIndexWritable that = (TileIndexWritable) o;
        int result = compare(this.tileY, that.tileY);
        if (result == 0) {
            result = compare(this.tileX, that.tileX);
        }
        return result;
    }

    private int compare(int thisInt, int thatInt) {
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
        return this.tileX == other.tileX && this.tileY == other.tileY;
    }


    public int hashCode() {
        return tileX + tileY * 1000;
    }

    public String toString() {
        return new StringBuilder().append(tileX).append(",").append(tileY).toString();
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
            int thisTileY = readInt(b1, s1);
            int thatTileY = readInt(b2, s2);

            int result = compare(thisTileY, thatTileY);
            if (result == 0) {
                int thisTileX = readInt(b1, s1 + 4);
                int thatTileX = readInt(b2, s2 + 4);
                result = compare(thisTileX, thatTileX);
            }
            return result;
        }
    }

    // register this comparator
    static {
        WritableComparator.define(TileIndexWritable.class, new Comparator());
    }
}
