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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions the bins by their bin index.
 * Reduces will receive spatial bins of contiguous latitude ranges.
 *
 * @author Tom Block
 */
public class IsinPartitioner extends Partitioner<LongWritable, L3SpatialBin> implements Configurable {

    private static final int NUM_TILE_COLUMNS = 36;

    private Configuration conf;


    @Override
    public int getPartition(LongWritable binIndex, L3SpatialBin spatialBin, int numPartitions) {
        long idx = binIndex.get();

        final int tile_y = (int) (idx / 10000000000L);
        long partIndex = idx - 10000000000L * tile_y;
        final int tile_x = (int) (partIndex / 100000000L);

        return (tile_y * NUM_TILE_COLUMNS + tile_x) % numPartitions;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
