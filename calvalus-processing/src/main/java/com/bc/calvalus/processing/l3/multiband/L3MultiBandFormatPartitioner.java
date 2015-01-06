package com.bc.calvalus.processing.l3.multiband;

import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionBinIndex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  The partitioner for for formatting
 *  multiple regions of a Binning product at once.
 */
public class L3MultiBandFormatPartitioner extends Partitioner<L3MultiRegionBinIndex, FloatWritable> {
    @Override
    public int getPartition(L3MultiRegionBinIndex l3MultiRegionBinIndex, FloatWritable binValue, int numPartitions) {
        return l3MultiRegionBinIndex.getRegionIndex();
    }

}
