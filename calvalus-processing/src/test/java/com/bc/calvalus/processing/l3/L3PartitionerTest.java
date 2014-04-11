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

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.esa.beam.binning.PlanetaryGrid;
import org.esa.beam.binning.operator.BinningConfig;
import org.junit.Test;

import static org.junit.Assert.*;

public class L3PartitionerTest {

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalConfig() {
        createPartitioner(113, "");
        fail("IllegalArgumentException?");
    }

    @Test
    public void test6Rows2Partitions() {
        L3Partitioner l3Partitioner = createPartitioner(6, "");
        PlanetaryGrid planetaryGrid = l3Partitioner.getPlanetaryGrid();

        assertEquals(3, planetaryGrid.getNumCols(0));
        assertEquals(8, planetaryGrid.getNumCols(1));
        assertEquals(12, planetaryGrid.getNumCols(2));

        assertEquals(12, planetaryGrid.getNumCols(3));
        assertEquals(8, planetaryGrid.getNumCols(4));
        assertEquals(3, planetaryGrid.getNumCols(5));

        int numPartitions = 2;
        //row 1
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(0), null, numPartitions));
        //row 2
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8 - 1), null, numPartitions));
        //row 3
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 - 1), null, numPartitions));
        ///////////////////
        //row 4
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 - 1), null, numPartitions));
        //row 5
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12), null, numPartitions));
        //row 6
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8 + 3 - 1), null, numPartitions));
    }

    @Test
    public void test6Rows3Partitions() {
        L3Partitioner l3Partitioner = createPartitioner(6, "");
        PlanetaryGrid planetaryGrid = l3Partitioner.getPlanetaryGrid();

        assertEquals(3, planetaryGrid.getNumCols(0));
        assertEquals(8, planetaryGrid.getNumCols(1));

        assertEquals(12, planetaryGrid.getNumCols(2));
        assertEquals(12, planetaryGrid.getNumCols(3));

        assertEquals(8, planetaryGrid.getNumCols(4));
        assertEquals(3, planetaryGrid.getNumCols(5));

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
        L3Partitioner l3Partitioner = createPartitioner(8, "");
        PlanetaryGrid planetaryGrid = l3Partitioner.getPlanetaryGrid();

        assertEquals(3, planetaryGrid.getNumCols(0));
        assertEquals(9, planetaryGrid.getNumCols(1));
        assertEquals(13, planetaryGrid.getNumCols(2));

        assertEquals(16, planetaryGrid.getNumCols(3));
        assertEquals(16, planetaryGrid.getNumCols(4));
        assertEquals(13, planetaryGrid.getNumCols(5));

        assertEquals(9, planetaryGrid.getNumCols(6));
        assertEquals(3, planetaryGrid.getNumCols(7));

        int numPartitions = 3;

        assertEquals(0, l3Partitioner.getPartition(new LongWritable(0), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13 - 1), null, numPartitions));

        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13 + 16 + 16 + 13 - 1), null, numPartitions));

        assertEquals(2, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13 + 16 + 16 + 13), null, numPartitions));
        assertEquals(2, l3Partitioner.getPartition(new LongWritable(3 + 9 + 13 + 16 + 16 + 13 + 9 + 3 - 1), null, numPartitions));
    }

    @Test
    public void testPartitionOfGeoRegion_6Rows2Partitions() throws Exception {
        L3Partitioner l3Partitioner = createPartitioner(6, "polygon((0.0 -31.0, 180.00 -31.0, 180.0 -90.0, 0.0 -90.00, 0.0 -31.0))");
        int numPartitions = 2;

        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8 - 1), null, numPartitions));

        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new LongWritable(3 + 8 + 12 + 12 + 8 + 3 - 1), null, numPartitions));
    }

    @Test
    public void testLCCase() throws Exception {
        L3Partitioner l3Partitioner = createPartitioner(66792, "polygon((-7 54, -7 38.5, 5.5 38.5, 5.5 54, -7 54))");
        int numPartitions = 4;
        assertEquals(3, l3Partitioner.getPartition(new LongWritable(1075911823), null, numPartitions));

    }

    private static L3Partitioner createPartitioner(int numRows, String wkt) {
        L3Partitioner l3Partitioner = new L3Partitioner();
        Configuration configuration = new Configuration();

        BinningConfig l3Config = new BinningConfig();
        l3Config.setNumRows(numRows);
        configuration.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3Config.toXml());

        configuration.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, wkt);
        l3Partitioner.setConf(configuration);

        return l3Partitioner;
    }


}
