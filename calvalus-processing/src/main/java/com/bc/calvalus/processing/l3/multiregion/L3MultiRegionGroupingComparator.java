package com.bc.calvalus.processing.l3.multiregion;

import org.apache.hadoop.io.WritableComparator;

/**
 * A Comparator for grouping L3MultiRegionBinIndex by region.
 */
public class L3MultiRegionGroupingComparator extends WritableComparator {
    public L3MultiRegionGroupingComparator() {
        super(L3MultiRegionBinIndex.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        int thisRegionIndex = readInt(b1, s1);
        int thatRegionIndex = readInt(b2, s2);
        return L3MultiRegionBinIndex.compareInts(thisRegionIndex, thatRegionIndex);
    }
}
