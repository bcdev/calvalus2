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

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class MosaicPartitionerTest {

    @Test
    public void testGlobal() {
        MosaicPartitioner partitioner = new MosaicPartitioner();
        assertEquals(0, partitioner.getPartition(new TileIndexWritable(0, 0, 34, 0), null, 4));
        assertEquals(0, partitioner.getPartition(new TileIndexWritable(0, 0, 34, 1), null, 4));

        assertEquals(1, partitioner.getPartition(new TileIndexWritable(0, 1,34, 45), null, 4));
        assertEquals(1, partitioner.getPartition(new TileIndexWritable(0, 1, 34, 89), null, 4));

        assertEquals(2, partitioner.getPartition(new TileIndexWritable(0, 2, 34, 90), null, 4));
        assertEquals(2, partitioner.getPartition(new TileIndexWritable(0, 2, 34, 134), null, 4));

        assertEquals(3, partitioner.getPartition(new TileIndexWritable(0, 3, 34, 135), null, 4));
        assertEquals(3, partitioner.getPartition(new TileIndexWritable(0, 3, 34, 136), null, 4));
        assertEquals(3, partitioner.getPartition(new TileIndexWritable(0, 3, 34, 180), null, 4));
    }

}
