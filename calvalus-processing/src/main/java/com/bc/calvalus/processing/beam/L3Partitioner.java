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

import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.SpatialBin;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions the bins by their bin index.
 * Reduces will recive spatial bins of contiguous latitude ranges.
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class L3Partitioner extends Partitioner<LongWritable, SpatialBin> implements Configurable {

    private Configuration conf;
    private BinningGrid binningGrid;

    @Override
    public int getPartition(LongWritable binIndex, SpatialBin spatialBin, int numPartitions) {
        long idx = binIndex.get();
        int row = binningGrid.getRowIndex(idx);
        return (row * numPartitions) / binningGrid.getNumRows();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String level3Parameters = new ProcessingConfiguration(conf).getLevel3Parameters();
        BeamL3Config l3Config = BeamL3Config.create(level3Parameters);
        setL3Config(l3Config);
    }

    void setL3Config(BeamL3Config l3Config) {
        this.binningGrid = l3Config.getBinningGrid();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    BinningGrid getBinningGrid() {
        return binningGrid;
    }
}
