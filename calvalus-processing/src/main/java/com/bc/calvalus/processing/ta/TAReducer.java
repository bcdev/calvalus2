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

import org.esa.beam.binning.BinManager;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3TemporalBin;
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
public class TAReducer extends Reducer<Text, L3TemporalBin, Text, TAPoint> implements Configurable {
    private Configuration conf;
    private BinManager binManager;
    private String minDate;
    private String maxDate;

    @Override
    protected void reduce(Text regionName, Iterable<L3TemporalBin> bins, Context context) throws IOException, InterruptedException {
        context.write(regionName, computeTaPoint(regionName.toString(), bins));
    }

    TAPoint computeTaPoint(String regionName, Iterable<L3TemporalBin> bins) {
        L3TemporalBin outputBin = (L3TemporalBin) binManager.createTemporalBin(-1);
        for (L3TemporalBin bin : bins) {
            binManager.aggregateTemporalBin(bin, outputBin);
        }
        return new TAPoint(regionName, minDate, maxDate, outputBin);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.binManager = L3Config.get(conf).createBinningContext(null).getBinManager();
        minDate = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
        maxDate = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
