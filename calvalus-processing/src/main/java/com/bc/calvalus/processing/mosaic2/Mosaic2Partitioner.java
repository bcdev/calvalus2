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

package com.bc.calvalus.processing.mosaic2;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.mosaic.MosaicGrid;
import com.bc.calvalus.processing.mosaic.TileDataWritable;
import com.bc.calvalus.processing.mosaic.TileIndexWritable;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.esa.snap.binning.operator.BinningConfig;

/**
 * Partitions the tiles by their y index, and optionally by their x index.
 *
 * @author Martin
 */
public class Mosaic2Partitioner extends Partitioner<TileIndexWritable, L3SpatialBinMicroTileWritable> implements Configurable {

    private Configuration conf;
    private int numXPartitions;
    private int macroTileRows;
    private int macroTileCols;

    @Override
    public int getPartition(TileIndexWritable tileIndex, L3SpatialBinMicroTileWritable tileData, int numPartitions) {
        int partition = numPartitions * tileIndex.getMacroTileY() / macroTileRows + numXPartitions * tileIndex.getMacroTileX() / macroTileCols;
        if (partition < 0) {
            partition = 0;
        } else if (partition >= numPartitions) {
            partition = numPartitions - 1;
        }
        return partition;
    }

    @Override
    public void setConf(Configuration conf) {
        try {
            this.conf = conf;
            numXPartitions = conf.getInt("calvalus.mosaic.numXPartitions", 1);
            final String l3ConfXML = conf.get(JobConfigNames.CALVALUS_CELL_PARAMETERS, conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
            final BinningConfig binningConfig = BinningConfig.fromXml(l3ConfXML);
            final int numRowsGlobal = binningConfig.getNumRows();
            final int macroTileHeight = conf.getInt("tileHeight", conf.getInt("tileSize", numRowsGlobal));
            final int macroTileWidth = conf.getInt("tileWidth", conf.getInt("tileSize", numRowsGlobal * 2));
            macroTileRows = numRowsGlobal / macroTileHeight;
            macroTileCols = 2 * numRowsGlobal / macroTileWidth;
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid L3 configuration: " + e.getMessage(), e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
