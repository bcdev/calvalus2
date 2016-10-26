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

package com.bc.calvalus.processing.ra;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The reducer for the region analysis workflow
 *
 * @author MarcoZ
 */
public class RAReducer extends Reducer<ExtractKey, ExtractValue, NullWritable, NullWritable> {

    private RAConfig raConfig;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.raConfig = RAConfig.get(context.getConfiguration());
        RAConfig raConfig = this.raConfig;
    }

    // key == region
    // values == ordered by time, extracts
    @Override
    protected void reduce(ExtractKey key, Iterable<ExtractValue> values, Context context) throws IOException, InterruptedException {
        // write netcdf file
        // compute stats
        // write CSV


        int regionId = key.getRegionId();
        String regionName = raConfig.getRegions()[regionId].getName();
        System.out.println("regionName = " + regionName);


        // open netcdf
        // NFileWriteable nFileWriteable = N4FileWriteable.create("region " + regionId);
        for (ExtractValue extract : values) {
            long time = extract.getTime();
            // if new agg window
            // - close old window
            // - open new widow
            // update statistics
            // write to netcdf
        }
        // close old agg window
        // close netcdf

    }
}
