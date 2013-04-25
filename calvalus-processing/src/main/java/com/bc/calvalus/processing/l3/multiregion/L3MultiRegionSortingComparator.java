package com.bc.calvalus.processing.l3.multiregion;

import org.apache.hadoop.io.WritableComparator;

/**
 * A Comparator optimized for L3MultiRegionBinIndex.
 */
public class L3MultiRegionSortingComparator extends WritableComparator {

    public L3MultiRegionSortingComparator() {
        super(L3MultiRegionBinIndex.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        int thisRegionIndex = readInt(b1, s1);
        int thatRegionIndex = readInt(b2, s2);
        int result = L3MultiRegionBinIndex.compareInts(thisRegionIndex, thatRegionIndex);
        if (result == 0) {
            long thisBinIndex = readLong(b1, s1 + 4);
            long thatBinIndex = readLong(b2, s2 + 4);
            result = L3MultiRegionBinIndex.compareLongs(thisBinIndex, thatBinIndex);
        }
        return result;
    }
}
