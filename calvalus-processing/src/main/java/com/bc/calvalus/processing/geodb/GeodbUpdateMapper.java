/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.geodb;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.inventory.search.SafeUpdateInventory;
import com.bc.inventory.search.StreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * A mapper for updating the product-DB
 */
public class GeodbUpdateMapper extends Mapper<NullWritable, NullWritable, Text, Text> {
    
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        updateInventory(context, conf);
    }

    static void updateInventory(TaskInputOutputContext context, Configuration conf) throws IOException {
        String geoInventory = conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY);
        StreamFactory streamFactory = new HDFSStreamFactory(conf);
        SafeUpdateInventory inventory = new SafeUpdateInventory(streamFactory, geoInventory);
        inventory.setUpdatePrefix("scan.");
        inventory.setAtticPrefix("scan.");
        inventory.setAtticSuffix("_" + context.getJobID().toString() + ".csv");
        inventory.setVerbose(true);
        int addedProducts = inventory.updateIndex();
        System.out.println("updated index. Added products = " + addedProducts);
    }
}
