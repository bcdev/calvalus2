package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.SpatialBin;
import com.bc.calvalus.b3.TemporalBin;
import com.bc.calvalus.b3.VariableContextImpl;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces list of spatial bins to a temporal bin.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Reducer extends Reducer<IntWritable, SpatialBin, IntWritable, TemporalBin> implements Configurable {

    private Configuration conf;
    private BinManager binManager;

    @Override
    protected void reduce(IntWritable binIndex, Iterable<SpatialBin> spatialBins, Context context) throws IOException, InterruptedException {
        TemporalBin temporalBin = binManager.createTemporalBin(binIndex.get());
        for (SpatialBin spatialBin : spatialBins) {
            binManager.aggregateTemporalBin(spatialBin, temporalBin);
        }
        context.write(binIndex, temporalBin);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.binManager = L3Config.getBinManager(conf);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
 }
