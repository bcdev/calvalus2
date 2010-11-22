package com.bc.calvalus.binning;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition the bins by their binIndex.
 *
 * @author Marco Zuehlke
 */
public class L3Partitioner extends Partitioner<IntWritable, SpatialBin> implements Configurable {

    private Configuration conf;
    private IsinBinningGrid binningGrid;

    @Override
    public int getPartition(IntWritable binIndex, SpatialBin spatialBin, int numPartitions) {
        int idx = binIndex.get();
        int row = binningGrid.getRowIndex(idx);
        return (row * numPartitions) / binningGrid.getNumRows();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        int numRows = conf.getInt(L3ProcessingMapper.CONFNAME_L3_NUM_ROWS, -1);
        binningGrid = new IsinBinningGrid(numRows);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    IsinBinningGrid getBinningGrid() {
        return binningGrid;
    }
}
