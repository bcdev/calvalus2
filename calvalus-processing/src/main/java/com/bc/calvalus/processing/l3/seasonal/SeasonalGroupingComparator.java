package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionBinIndex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A Comparator for band and tile numbers.
 */
public class SeasonalGroupingComparator extends WritableComparator {

    public SeasonalGroupingComparator() {
        super(IntWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        int bandAndTile1 = readInt(b1, s1);
        int bandAndTile2 = readInt(b2, s2);
        return compareInts(bandAndTile1 << 16, bandAndTile2 << 16);
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
}
