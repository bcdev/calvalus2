package com.bc.calvalus.binning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces list of spatial bins to a temporal bin.
 *
 * @author Norman Fomferra
 */
public class L3ProcessingReducer extends Reducer<IntWritable, SpatialBin, IntWritable, TemporalBin> {

    @Override
    protected void reduce(IntWritable binIndex, Iterable<SpatialBin> spatialBins, Context context) throws IOException, InterruptedException {
        TemporalBin temporalBin = new TemporalBin(binIndex.get());
        for (SpatialBin spatialBin : spatialBins) {
            temporalBin.addBin(spatialBin);
        }
        context.write(binIndex, temporalBin);
    }
}
