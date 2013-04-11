package com.bc.calvalus.processing.l3;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  The partitioner for for formatting
 *  multiple regions of a Binning product at once.
 */
public class L3MultiRegionFormatPartitioner extends Partitioner<L3RegionBinIndex, L3TemporalBin> {
    @Override
    public int getPartition(L3RegionBinIndex l3RegionBinIndex, L3TemporalBin l3TemporalBin, int numPartitions) {
        return l3RegionBinIndex.getRegionIndex() %  numPartitions;
    }

}
