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

import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3Partitioner;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static org.junit.Assert.*;

public class L3PartitionerTest {

    @Test
    public void testIllegalConfig() {
        L3Partitioner l3Partitioner = new L3Partitioner();

        assertNull(l3Partitioner.getBinningGrid());
        L3Config l3Config = new L3Config();
        l3Config.numRows = 113;
        try {
            l3Partitioner.setL3Config(l3Config);
            fail("IllegalArgumentException?");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void test6Rows2Partitions() {
        L3Partitioner l3Partitioner = new L3Partitioner();
        L3Config l3Config = new L3Config();
        int numRows = 6;
        l3Config.numRows = numRows;
        l3Partitioner.setL3Config(l3Config);
        BinningGrid binningGrid = l3Partitioner.getBinningGrid();

        assertEquals(3, binningGrid.getNumCols(0));
        assertEquals(8, binningGrid.getNumCols(1));
        assertEquals(12, binningGrid.getNumCols(2));

        assertEquals(12, binningGrid.getNumCols(3));
        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        int numPartitions = 2;

        assertEquals(0, l3Partitioner.getPartition(new LongWritable(0), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8 - 1), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 - 1), null, numPartitions));

        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 - 1), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8 + 3 - 1), null, numPartitions));
    }

    @Test
    public void test6Rows3Partitions() {
        L3Partitioner l3Partitioner = new L3Partitioner();
        L3Config l3Config = new L3Config();
        int numRows = 6;
        l3Config.numRows = numRows;
        l3Partitioner.setL3Config(l3Config);
        BinningGrid binningGrid = l3Partitioner.getBinningGrid();

        assertEquals(3, binningGrid.getNumCols(0));
        assertEquals(8, binningGrid.getNumCols(1));

        assertEquals(12, binningGrid.getNumCols(2));
        assertEquals(12, binningGrid.getNumCols(3));

        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        int numPartitions = 3;

        assertEquals(0, l3Partitioner.getPartition(new LongWritable(0), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8 - 1), null, numPartitions));

        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 - 1), null, numPartitions));

        assertEquals(2, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12), null, numPartitions));
        assertEquals(2, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8), null, numPartitions));
        assertEquals(2, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8 + 3 - 1), null, numPartitions));
    }

    @Test
    public void test8Rows3Partitions() {
        L3Partitioner l3Partitioner = new L3Partitioner();
        L3Config l3Config = new L3Config();
        int numRows = 8;
        l3Config.numRows = numRows;
        l3Partitioner.setL3Config(l3Config);
        BinningGrid binningGrid = l3Partitioner.getBinningGrid();

        assertEquals(3, binningGrid.getNumCols(0));
        assertEquals(9, binningGrid.getNumCols(1));
        assertEquals(13, binningGrid.getNumCols(2));

        assertEquals(16, binningGrid.getNumCols(3));
        assertEquals(16, binningGrid.getNumCols(4));
        assertEquals(13, binningGrid.getNumCols(5));

        assertEquals(9, binningGrid.getNumCols(6));
        assertEquals(3, binningGrid.getNumCols(7));

        int numPartitions = 3;

        assertEquals(0, l3Partitioner.getPartition(new LongWritable(0), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13 - 1), null, numPartitions));

        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13 + 16 + 16 + 13 - 1), null, numPartitions));

        assertEquals(2, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13 + 16 + 16 + 13), null, numPartitions));
        assertEquals(2, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13 + 16 + 16 + 13 + 9 + 3 -1), null, numPartitions));
    }
}
