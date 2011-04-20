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

package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.processing.JobConfNames;
import com.bc.calvalus.processing.l3.L3Config;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces list of spatial bins to a temporal bin.
 *
 * @author Norman Fomferra
 */
public class TAReducer extends Reducer<Text, TemporalBin, Text, TAPoint> implements Configurable {
    private Configuration conf;
    private BinManager binManager;

    @Override
    protected void reduce(Text regionName, Iterable<TemporalBin> bins, Context context) throws IOException, InterruptedException {
        TemporalBin outputBin = binManager.createTemporalBin(-1);
        for (TemporalBin bin : bins) {
            binManager.aggregateTemporalBin(bin, outputBin);
        }
        context.write(regionName, new TAPoint(regionName.toString(),
                                              conf.get(JobConfNames.CALVALUS_MIN_DATE),
                                              conf.get(JobConfNames.CALVALUS_MAX_DATE),
                                              outputBin));
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String level3Parameters = conf.get(JobConfNames.CALVALUS_L3_PARAMETERS);
        L3Config l3Config = L3Config.fromXml(level3Parameters);
        this.binManager = l3Config.getBinningContext().getBinManager();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
