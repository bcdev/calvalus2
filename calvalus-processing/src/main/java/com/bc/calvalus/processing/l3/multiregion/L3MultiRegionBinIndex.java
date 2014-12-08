package com.bc.calvalus.processing.l3.multiregion;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  The key (regionID and binIndex) for for formatting
 *  multiple regions of a Binning product at once.
 */
public class L3MultiRegionBinIndex implements WritableComparable {

    private int regionIndex;
    private long binIndex;

    public L3MultiRegionBinIndex() {
    }

    public L3MultiRegionBinIndex(int regionIndex, long binIndex) {
        this.regionIndex = regionIndex;
        this.binIndex = binIndex;
    }

    public int getRegionIndex() {
        return regionIndex;
    }

    public long getBinIndex() {
        return binIndex;
    }

    public void setRegionIndex(int regionIndex) {
        this.regionIndex = regionIndex;
    }

    public void setBinIndex(long binIndex) {
        this.binIndex = binIndex;
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
        L3MultiRegionBinIndex that = (L3MultiRegionBinIndex) o;
        int result = compareInts(this.regionIndex, that.regionIndex);
        if (result == 0) {
            result = compareLongs(this.binIndex, that.binIndex);
        }
        return result;
    }

    static int compareInts(int thisInt, int thatInt) {
        if (thisInt < thatInt) {
            return -1;
        } else if (thisInt == thatInt) {
            return 0;
        } else {
            return 1;
        }
    }

    static int compareLongs(long thisInt, long thatInt) {
        if (thisInt < thatInt) {
            return -1;
        } else if (thisInt == thatInt) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public String toString() {
        return "L3MultiRegionBinIndex{" +
                "regionIndex=" + regionIndex +
                ", binIndex=" + binIndex +
                '}';
    }
}
