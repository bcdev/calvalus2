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

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ra.stat.RADateRanges;
import com.bc.calvalus.processing.ra.stat.RegionAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.ParseException;

/**
 * The reducer for the region analysis workflow
 *
 * @author MarcoZ
 */
public class RAReducer extends Reducer<RAKey, RAValue, NullWritable, NullWritable> {

    private RAConfig raConfig;
    private RegionAnalysis regionAnalysis;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.raConfig = RAConfig.get(conf);
        String dateRangesString = conf.get(JobConfigNames.CALVALUS_RA_DATE_RANGES);
        RADateRanges dateRanges;
        try {
            dateRanges = RADateRanges.create(dateRangesString);
        } catch (ParseException e) {
            throw new IOException(e);
        }

        regionAnalysis = new RegionAnalysis(dateRanges, raConfig.getBandConfigs()) {

            @Override
            public Writer createWriter(String fileName) throws IOException, InterruptedException {
                Path path = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);
                return new OutputStreamWriter(path.getFileSystem(context.getConfiguration()).create(path));
            }
        };
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        regionAnalysis.close();
    }

    // key == region
    // values == extracts, ordered by time
    @Override
    protected void reduce(RAKey key, Iterable<RAValue> values, Context context) throws IOException, InterruptedException {
        // 1) write netCDF file
        // 2) compute stats
        // 3) write CSV

        final int regionId = key.getRegionId(); // TODO is this needed
        final String regionName = key.getRegionName();
        System.out.println("regionName = " + regionName);

        // open netCDF
        // NFileWriteable nFileWriteable = N4FileWriteable.create("region " + regionId);
        regionAnalysis.startRegion(regionName);

        for (RAValue extract : values) {
            long time = extract.getTime();
            int numPixel = extract.getNumPixel();
            float[][] samples = extract.getSamples();
            System.out.println("statistics.addData time = " + time + " numPixel = " + numPixel + "  numSamples = " + extract.getSamples()[0].length);
            regionAnalysis.addData(time, numPixel, samples);
        }
        regionAnalysis.endRegion();
        // close netCDF
    }
}
