/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing;

import com.bc.calvalus.processing.executable.ExecutableProcessorAdapter;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.Rectangle;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ProcessorAdapterTest {

    @Test
    public void testGetDatePart() throws Exception {
        assertEquals("2005/06/09", ProcessorAdapter.getDatePart(new Path("/foo/2005/06/09/MER.N1")));
        assertEquals("2005/06/09", ProcessorAdapter.getDatePart(new Path("foo/2005/06/09/MER.N1")));
        assertEquals("2005/06/09", ProcessorAdapter.getDatePart(new Path("/2005/06/09/MER.N1")));
        assertEquals("2005/06/09", ProcessorAdapter.getDatePart(new Path("2005/06/09/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("/06/09/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("/09/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("09/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("MER.N1")));
    }

    @Ignore
    @Test
    public void testShallowCopyPatches() throws Exception {
        ProcessorAdapter.shallowCopyPatches("/home/boe/tmp/caltest");
    }

    @Ignore
    @Test
    public void testGetInputRectangle() throws IOException {
        Configuration conf = new Configuration();
        ProductSplit productSplit = new ProductSplit(new Path("/windows/tmp/MSIL1C-60m-7blk-36T-A-107-20210718-v01.2.nc"), 1887624736L, new String[] { "localhost" }, -1, -1);
        conf.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, "POLYGON((30 40,36 40,36 48,30 48,30 40))");
        MapTask mapTask = new MapTask();
        StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();
        MapContext mapContext = new MapContextImpl(conf, mapTask.getTaskID(), null, null, null, reporter, productSplit);
        ExecutableProcessorAdapter processorAdapter = new ExecutableProcessorAdapter(mapContext);
        // cuts off northern part of product while getting border pixels
        // set breakpoint at GeoUtils:684         regionIntersection.apply(pixelRegionFinder);
        // set breakpoint at GeoUtils:778:        final PixelPos pixelPos = geoCoding.getPixelPos(geoPos, null);
        // getPixelPos fails for the northern border
        Rectangle inputRectangle = processorAdapter.getInputRectangle();
        System.out.println(inputRectangle);
    }

}