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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.TemporalBin;
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
public class L3TAReducer extends Reducer<IntWritable, TemporalBin, IntWritable, TemporalBin>  {

    @Override
    protected void reduce(IntWritable roiIndex, Iterable<TemporalBin> bins, Context context) throws IOException, InterruptedException {

        TemporalBin avgBin = null;
        int n = 0;

        for (TemporalBin bin : bins) {
            final int propertyCount = bin.getPropertyCount();
            if (avgBin == null) {
                avgBin = new TemporalBin(0, propertyCount);
            }
            avgBin.setNumObs(avgBin.getNumObs() + bin.getNumObs());
            avgBin.setNumPasses(avgBin.getNumPasses() + bin.getNumPasses());
            final float[] outputProperties = avgBin.getProperties();
            final float[] currentProperties = bin.getProperties();
            for (int i = 0; i < propertyCount; i++) {
                outputProperties[i] += currentProperties[i];
            }
            n++;
        }

        if (avgBin != null) {
            avgBin.setNumObs(avgBin.getNumObs() / n);
            avgBin.setNumPasses(avgBin.getNumPasses() / n);
            final float[] outputProperties = avgBin.getProperties();
            for (int i = 0; i < outputProperties.length; i++) {
                outputProperties[i] /= n;
            }
            context.write(roiIndex, avgBin);
        }
    }

}
