package com.bc.calvalus.processing.l3.multiregion;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  The partitioner for for formatting
 *  multiple regions of a Binning product at once.
 */
public class L3MultiRegionFormatPartitioner extends Partitioner<L3MultiRegionBinIndex, L3MultiRegionTemporalBin> {
    @Override
    public int getPartition(L3MultiRegionBinIndex l3MultiRegionBinIndex, L3MultiRegionTemporalBin l3TemporalBin, int numPartitions) {
        return l3MultiRegionBinIndex.getRegionIndex() %  numPartitions;
    }

}
