package com.bc.calvalus.processing.ta;

import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner that sorts the L3TemporalBins by their region index.
 * The partitioner works together with the TAKey comparators.
 * Reduce calls will get a set of all L3 bins for a region sorted by time and binIndex.
 *
 * @author Martin Boettcher
 */
public class TAPartitioner extends Partitioner<TAKey, L3TemporalBinWithIndex> implements Configurable {

    Configuration conf;
    int numRegions;

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public int getPartition(TAKey key, L3TemporalBinWithIndex value, int numReduceTasks) {
        return key.getRegionId() * numReduceTasks / numRegions;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        numRegions = conf.getInt("numRegions", 1);
    }
}
