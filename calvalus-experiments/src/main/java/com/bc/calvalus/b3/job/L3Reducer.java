package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.BinManagerImpl;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.SpatialBin;
import com.bc.calvalus.b3.TemporalBin;
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
    private BinManagerImpl binningManager;

    @Override
    protected void reduce(IntWritable binIndex, Iterable<SpatialBin> spatialBins, Context context) throws IOException, InterruptedException {
        TemporalBin temporalBin = binningManager.createTemporalBin(binIndex.get());
        for (SpatialBin spatialBin : spatialBins) {
            binningManager.aggregateTemporalBin(spatialBin, temporalBin);
        }
        context.write(binIndex, temporalBin);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        // todo - use config to construct the correct list of aggregators
        AggregatorAverage aggregator = new AggregatorAverage(new MyVariableContext(), "ndvi");
        binningManager = new BinManagerImpl(aggregator);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
 }
