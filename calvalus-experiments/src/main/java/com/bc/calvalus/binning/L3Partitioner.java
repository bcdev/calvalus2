package com.bc.calvalus.binning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition the bins by their binIndex.
 *
 * @author Marco Zuehlke
 */
public class L3Partitioner extends Partitioner<IntWritable, SpatialBin> {

  public int getPartition(IntWritable binIndex, SpatialBin spatialBin, int numReduceTasks) {
    return binIndex.get() / numReduceTasks;
  }

}
