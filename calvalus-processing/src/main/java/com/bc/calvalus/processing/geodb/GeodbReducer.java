/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.inventory.search.SafeUpdateInventory;
import com.bc.inventory.search.StreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;

/**
 * A reducer for generating entries for the product-DB
 */
public class GeodbReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

    private OutputStreamWriter scanResultWriter;
    private String scanFilename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String geoInventory = conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY);

        scanFilename = "scans/scan." + DateUtils.createDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());
        Path scanResultPath = new Path(geoInventory, scanFilename);
        scanResultWriter = new OutputStreamWriter(scanResultPath.getFileSystem(conf).create(scanResultPath));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String value = values.iterator().next().toString(); // there is exactly one value
        String scanRecord = key + "\t" + value + "\n";
        scanResultWriter.write(scanRecord);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        scanResultWriter.close();

        // read scanFile back in and update DB
        Configuration conf = context.getConfiguration();
        String geoInventory = conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY);
        StreamFactory streamFactory = new HDFSStreamFactory(conf);
        SafeUpdateInventory inventory = new SafeUpdateInventory(streamFactory, geoInventory);
        inventory.setVerbose(true);
        int addedProducts = inventory.updateIndex();
        System.out.println("updated index. Added products = " + addedProducts);
    }
}
