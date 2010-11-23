package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.BinManagerImpl;
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
 */
public class L3Reducer extends Reducer<IntWritable, SpatialBin, IntWritable, TemporalBin> implements Configurable {

    private Configuration conf;
    private IsinBinningGrid binningGrid;
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
        // todo - use config to construct a BinningGrid instance of the correct type
        int numRows = conf.getInt(L3Mapper.CONFNAME_L3_NUM_ROWS, -1);
        binningGrid = new IsinBinningGrid(numRows);
        // todo - use config to construct the correct list of aggregators
        AggregatorAverage aggregator = new AggregatorAverage(new MyVariableContext(), "ndvi");
        binningManager = new BinManagerImpl(aggregator);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    IsinBinningGrid getBinningGrid() {
        return binningGrid;
    }
 }
