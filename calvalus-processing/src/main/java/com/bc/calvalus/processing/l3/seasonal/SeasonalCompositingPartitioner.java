package com.bc.calvalus.processing.l3.seasonal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  TBD
 */
public class SeasonalCompositingPartitioner extends Partitioner<IntWritable, BandTileWritable> {
    @Override
    public int getPartition(IntWritable bandAndTileNumber, BandTileWritable tile, int numPartitions) {
        return (bandAndTileNumber.get() >>> 22) & 0x1f;
    }
}
