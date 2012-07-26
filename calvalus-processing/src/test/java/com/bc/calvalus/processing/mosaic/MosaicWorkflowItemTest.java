/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import static org.junit.Assert.assertEquals;

public class MosaicWorkflowItemTest {

    @Test
    public void testComputeNumReducers() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid(6, 18, 20);
        Configuration conf = new Configuration();
        mosaicGrid.saveToConfiguration(conf);

        assertEquals(3 * 1, MosaicWorkflowItem.computeNumReducers(conf));

        conf.setInt("calvalus.mosaic.numXPartitions", 2);
        assertEquals(3 * 2, MosaicWorkflowItem.computeNumReducers(conf));
        conf.setInt("calvalus.mosaic.numXPartitions", 3);
        assertEquals(3 * 3, MosaicWorkflowItem.computeNumReducers(conf));
        conf.setInt("calvalus.mosaic.numXPartitions", 6);
        assertEquals(3 * 6, MosaicWorkflowItem.computeNumReducers(conf));

        mosaicGrid = new MosaicGrid();
        conf = new Configuration();
        mosaicGrid.saveToConfiguration(conf);
        assertEquals(36, MosaicWorkflowItem.computeNumReducers(conf));
        conf.setInt("calvalus.mosaic.numXPartitions", 4);
        assertEquals(36 * 4, MosaicWorkflowItem.computeNumReducers(conf));

    }
}
