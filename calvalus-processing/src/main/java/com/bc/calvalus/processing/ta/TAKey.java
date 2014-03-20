package com.bc.calvalus.processing.ta;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class TAKey implements WritableComparable {
    int regionId;
    long time;
    long binIndex;

    public TAKey(int regionId, long time, long binIndex) {
        this.regionId = regionId;
        this.time = time;
        this.binIndex = binIndex;
    }

    public TAKey() {}

    public int getRegionId() {
        return regionId;
    }

    public long getTime() { return time; }

    public long getBinIndex() {
        return binIndex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(regionId);
        out.writeLong(time);
        out.writeLong(binIndex);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        regionId = in.readInt();
        time = in.readLong();
        binIndex = in.readLong();
    }

    @Override
    public int compareTo(Object o) {
        TAKey taKey2 = (TAKey) o;
        //int compare = Integer.compare(getRegionId(), taKey2.getRegionId());
        int compare = compare(getRegionId(), taKey2.getRegionId());
        if (compare == 0) {
            //compare = Long.compare(getTime(), taKey2.getTime());
            compare = compare(getTime(), taKey2.getTime());
            if (compare == 0) {
                //compare = Long.compare(getBinIndex(), taKey2.getBinIndex());
                compare = compare(getBinIndex(), taKey2.getBinIndex());
            }
        }
        return compare;
    }

    // required for Java 1.6
    private static int compare(int v1, int v2) {
        if (v1 < v2) {
            return -1;
        } else if (v1 > v2) {
            return 1;
        } else {
            return 0;
        }
    }

    // required for Java 1.6
    private static int compare(long v1, long v2) {
        if (v1 < v2) {
            return -1;
        } else if (v1 > v2) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "TAKey(" + getRegionId() + "," + getTime() + "," + getBinIndex() + ")";
    }

    static class TAKeyComparator extends WritableComparator {

        protected TAKeyComparator() {
            super(TAKey.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            return ((TAKey) key1).compareTo(key2);
        }
    }

    static class TAKeyRegionComparator extends WritableComparator {

        protected TAKeyRegionComparator() {
            super(TAKey.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            //return Integer.compare(((TAKey) key1).getRegionId(), ((TAKey) key2).getRegionId());
            return TAKey.compare(((TAKey) key1).getRegionId(), ((TAKey) key2).getRegionId());
        }
    }

}
