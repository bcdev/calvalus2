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

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class MosaicPartitionerTest {

    @Test
    public void testOnlyRows() {
        MosaicPartitioner partitioner = new MosaicPartitioner();
        partitioner.setConf(new Configuration());
        assertEquals(0, getPartition(partitioner, 0, 0, 4));
        assertEquals(1, getPartition(partitioner, 0, 1, 4));
        assertEquals(2, getPartition(partitioner, 0, 2, 4));
        assertEquals(3, getPartition(partitioner, 0, 3, 4));

        assertEquals(0, getPartition(partitioner, 1, 0, 4));
        assertEquals(1, getPartition(partitioner, 1, 1, 4));
        assertEquals(2, getPartition(partitioner, 1, 2, 4));
        assertEquals(3, getPartition(partitioner, 1, 3, 4));

        assertEquals(0, getPartition(partitioner, 2, 0, 4));
        assertEquals(1, getPartition(partitioner, 2, 1, 4));
        assertEquals(2, getPartition(partitioner, 2, 2, 4));
        assertEquals(3, getPartition(partitioner, 2, 3, 4));

        assertEquals(3, getPartition(partitioner, 2, 4, 4));
        assertEquals(3, getPartition(partitioner, 2, 5, 4));
    }

    @Test
    public void testByMacroTile() {
        MosaicGrid mosaicGrid = new MosaicGrid(6, 18, 20);
        Configuration conf = new Configuration();
        mosaicGrid.saveToConfiguration(conf);
        conf.setInt("calvalus.mosaic.numXPartitions", 6);

        MosaicPartitioner partitioner = new MosaicPartitioner();
        partitioner.setConf(conf);
        final int numPartitions = 3 * 6;

        assertEquals(0, getPartition(partitioner, 0, 0, numPartitions));
        assertEquals(1, getPartition(partitioner, 1, 0, numPartitions));
        assertEquals(2, getPartition(partitioner, 2, 0, numPartitions));
        assertEquals(3, getPartition(partitioner, 3, 0, numPartitions));
        assertEquals(4, getPartition(partitioner, 4, 0, numPartitions));
        assertEquals(5, getPartition(partitioner, 5, 0, numPartitions));

        assertEquals(6, getPartition(partitioner, 0, 1, numPartitions));
        assertEquals(7, getPartition(partitioner, 1, 1, numPartitions));
        assertEquals(8, getPartition(partitioner, 2, 1, numPartitions));
        assertEquals(9, getPartition(partitioner, 3, 1, numPartitions));
        assertEquals(10, getPartition(partitioner, 4, 1, numPartitions));
        assertEquals(11, getPartition(partitioner, 5, 1, numPartitions));
    }

    @Test
    public void testTwoPerRow() {
        MosaicGrid mosaicGrid = new MosaicGrid(6, 18, 20);
        Configuration conf = new Configuration();
        mosaicGrid.saveToConfiguration(conf);
        conf.setInt("calvalus.mosaic.numXPartitions", 2);

        MosaicPartitioner partitioner = new MosaicPartitioner();
        partitioner.setConf(conf);
        final int numPartitions = 3 * 2;

        assertEquals(0, getPartition(partitioner, 0, 0, numPartitions));
        assertEquals(0, getPartition(partitioner, 1, 0, numPartitions));
        assertEquals(0, getPartition(partitioner, 2, 0, numPartitions));
        assertEquals(1, getPartition(partitioner, 3, 0, numPartitions));
        assertEquals(1, getPartition(partitioner, 4, 0, numPartitions));
        assertEquals(1, getPartition(partitioner, 5, 0, numPartitions));

        assertEquals(2, getPartition(partitioner, 0, 1, numPartitions));
        assertEquals(2, getPartition(partitioner, 1, 1, numPartitions));
        assertEquals(2, getPartition(partitioner, 2, 1, numPartitions));
        assertEquals(3, getPartition(partitioner, 3, 1, numPartitions));
        assertEquals(3, getPartition(partitioner, 4, 1, numPartitions));
        assertEquals(3, getPartition(partitioner, 5, 1, numPartitions));
    }

    private int getPartition(MosaicPartitioner partitioner, int macroTileX, int macroTileY, int numPartitions) {
        // tileX and tileY are not ued in the tested partitioner
        return partitioner.getPartition(new TileIndexWritable(macroTileX, macroTileY, 42, 42), null, numPartitions);
    }

}
