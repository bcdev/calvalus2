/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

import java.awt.Rectangle;

/**
 * Partitions the tiles by their y index.
 * Reduces will receive tiles of contiguous latitude ranges.
 *
 * @author Marco Zuehlke
 */
public class MosaicPartitioner extends Partitioner<TileIndexWritable, TileDataWritable> implements Configurable {

    private Configuration conf;
    private MosaicGrid mosaicGrid = null;
    private int numXPartitions;
    private int tileXDivisor;

    @Override
    public int getPartition(TileIndexWritable tileIndex, TileDataWritable tileData, int numPartitions) {
        int partition;
        if (mosaicGrid != null) {
            partition = mosaicGrid.getPartition(tileIndex.getMacroTileX(), tileIndex.getMacroTileY());
        } else {
            partition = tileIndex.getMacroTileY();
        }
        if (partition < 0) {
            partition = 0;
        } else if (partition >= numPartitions) {
            partition = numPartitions - 1;
        }
        if (mosaicGrid == null && numXPartitions > 1) {
            partition = partition * numXPartitions + tileIndex.getMacroTileX() / tileXDivisor;
        }
        return partition;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        mosaicGrid = MosaicGrid.create(conf);
        numXPartitions = conf.getInt("calvalus.mosaic.numXPartitions", 1);
        tileXDivisor = mosaicGrid.getNumMacroTileX() / numXPartitions;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
