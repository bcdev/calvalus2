/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.TemporalBin;
import org.esa.beam.binning.TemporalBinner;
import org.esa.beam.binning.cellprocessor.CellProcessorChain;

import java.io.IOException;

/**
 * Reduces list of spatial bins to a temporal bin.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Reducer extends Reducer<LongWritable, L3SpatialBin, LongWritable, L3TemporalBin> implements Configurable {

    private Configuration conf;
    private TemporalBinner temporalBinner;
    private CellProcessorChain cellChain;
    private boolean computeOutput;

    @Override
    protected void reduce(LongWritable binIndex, Iterable<L3SpatialBin> spatialBins, Context context) throws IOException, InterruptedException {
        final long idx = binIndex.get();
        TemporalBin temporalBin = temporalBinner.processSpatialBins(idx, spatialBins);

        if (computeOutput) {
            temporalBin = temporalBinner.computeOutput(idx, temporalBin);
            temporalBin = cellChain.process(temporalBin);
        }

        context.write(binIndex, (L3TemporalBin) temporalBin);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        computeOutput = conf.getBoolean(JobConfigNames.CALVALUS_L3_COMPUTE_OUTPUTS, true);
        BinningContext binningContext = L3Config.get(conf).createBinningContext();
        temporalBinner = new TemporalBinner(binningContext);
        cellChain = new CellProcessorChain(binningContext);

    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
