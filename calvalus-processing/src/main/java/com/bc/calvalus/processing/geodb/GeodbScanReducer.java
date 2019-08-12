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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A reducer for generating entries for the product-DB
 */
public class GeodbScanReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

    private OutputStreamWriter scanResultWriter = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        createResultWriter(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String value = values.iterator().next().toString(); // there is exactly one value
        String scanRecord = key + "\t" + value + "\n";
        if (scanResultWriter == null) {
            createResultWriter(context);
        }
        scanResultWriter.write(scanRecord);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (scanResultWriter != null) {
            scanResultWriter.close();

            Configuration conf = context.getConfiguration();
            if (conf.getBoolean(GeodbScanWorkflowItem.UPDATE_AFTER_SCAN_PROPERTY, false)) {
                GeodbUpdateMapper.updateInventory(context, conf);
            }
        }
    }

    private void createResultWriter(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String geoInventory = conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY);

        SimpleDateFormat dateFormat = DateUtils.createDateFormat("yyyy-MM-dd_HH-mm-ss_SSS");
        String scanFilename = "scan." + dateFormat.format(new Date()) + "__" + context.getJobID().toString() + ".csv";
        Path scanResultPath = new Path(geoInventory, scanFilename);
        System.out.println("scanResultPath = " + scanResultPath);
        scanResultWriter = new OutputStreamWriter(scanResultPath.getFileSystem(conf).create(scanResultPath));
    }
}
