package com.bc.calvalus.processing.l3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  The key (regionID and binIndex) for for formatting
 *  multiple regions of a Binning product at once.
 */
public class L3RegionBinIndex implements WritableComparable {

    private int regionIndex;
    private long binIndex;

    public L3RegionBinIndex(int regionIndex, long binIndex) {
        this.regionIndex = regionIndex;
        this.binIndex = binIndex;
    }

    public int getRegionIndex() {
        return regionIndex;
    }

    public long getBinIndex() {
        return binIndex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(regionIndex);
        out.writeLong(binIndex);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        regionIndex = in.readInt();
        binIndex = in.readLong();
    }

    public int compareTo(Object o) {
        L3RegionBinIndex that = (L3RegionBinIndex) o;
        int result = compareInts(this.regionIndex, that.regionIndex);
        if (result == 0) {
            result = compareLongs(this.binIndex, that.binIndex);
        }
        return result;
    }

    /**
     * A Comparator optimized for L3RegionBinIndex.
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(L3RegionBinIndex.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            int thisRegionIndex = readInt(b1, s1);
            int thatRegionIndex = readInt(b2, s2);
            int result = compareInts(thisRegionIndex, thatRegionIndex);
            if (result == 0) {
                long thisBinIndex = readLong(b1, s1 + 4);
                long thatBinIndex = readLong(b2, s2 + 4);
                result = compareLongs(thisBinIndex, thatBinIndex);
            }
            return result;
        }
    }

    // register this comparator
    static {
        WritableComparator.define(L3RegionBinIndex.class, new Comparator());
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

    private static int compareLongs(long thisInt, long thatInt) {
        if (thisInt < thatInt) {
            return -1;
        } else if (thisInt == thatInt) {
            return 0;
        } else {
            return 1;
        }
    }
}
