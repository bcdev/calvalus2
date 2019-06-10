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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.calvalus.processing.ra.stat.PixelArchiver;
import com.bc.calvalus.processing.ra.stat.RADateRanges;
import com.bc.calvalus.processing.ra.stat.RegionAnalysis;
import com.bc.calvalus.processing.ra.stat.WriterFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.ParseException;
import java.util.Date;
import java.util.logging.Logger;

/**
 * The reducer for the region analysis workflow
 *
 * @author MarcoZ
 */
public class RAReducer extends Reducer<RAKey, RAValue, NullWritable, NullWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    
    private RAConfig raConfig;
    private RegionAnalysis regionAnalysis;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.raConfig = RAConfig.get(conf);
        String dateRangesString = conf.get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES);
        boolean binValuesAsRatio = conf.getBoolean("calvalus.ra.binValuesAsRatio", false);
        RADateRanges dateRanges;
        try {
            dateRanges = RADateRanges.create(dateRangesString);
        } catch (ParseException e) {
            throw new IOException(e);
        }

        HdfsWriterFactory hdfsWriterFactory = new HdfsWriterFactory(context);
        regionAnalysis = new RegionAnalysis(dateRanges, raConfig, binValuesAsRatio, hdfsWriterFactory);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        regionAnalysis.close();
    }

    // key == region
    // values == extracts, ordered by time
    @Override
    protected void reduce(RAKey key, Iterable<RAValue> values, Context context) throws IOException, InterruptedException {

        final int regionId = key.getRegionId(); // TODO is this needed
        final String regionName = key.getRegionName();
        LOG.info("regionName: " + regionName);

        PixelArchiver pixelArchiver = null;
        if (raConfig.isWritePixelValues()) {
            pixelArchiver = new PixelArchiver(regionName, raConfig.getBandNames());
        }

        regionAnalysis.startRegion(regionId, regionName);

        long lastTime = -1;
        for (RAValue extract : values) {
            long time = extract.getTime();
            int numObs = extract.getNumObs();
            float[][] samples = extract.getSamples();
            int numSamples = samples[0].length;
            String productName = extract.getProductName();

            String timeFormatted = RADateRanges.dateFormat.format(new Date(time));
            LOG.info(String.format("    time: %s numObs: %8d  numSamples: %8d   %s", timeFormatted, numObs, numSamples, productName));

            regionAnalysis.addData(time, numObs, samples, productName);
            if (pixelArchiver != null) {
                if (time != lastTime && lastTime != -1) {
                    pixelArchiver.writeTempNetcdf();
                }
                pixelArchiver.addProductPixels(time, numObs, samples, productName);
                lastTime = time;
            }
        }
        regionAnalysis.endRegion();

        if (pixelArchiver != null) {
            pixelArchiver.writeTempNetcdf();   
            File[] netcdfFiles = pixelArchiver.createMergedNetcdf();
            Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
            for (File file : netcdfFiles) {
                InputStream is = new BufferedInputStream(new FileInputStream(file));
                Path workPath = new Path(workOutputPath, file.getName());
                OutputStream os = workPath.getFileSystem(context.getConfiguration()).create(workPath);
                ProductFormatter.copyAndClose(is, os, context);
            }
        }
    }

    private class HdfsWriterFactory implements WriterFactory {

        private final Context context;

        private HdfsWriterFactory(Context context) {
            this.context = context;
        }

        @Override
        public Writer createWriter(String filePath) throws IOException {
            Path workOutputPath = null;
            try {
                workOutputPath = FileOutputFormat.getWorkOutputPath(context);
            } catch (InterruptedException ie) {
                throw new IOException(ie);
            }
            Path path = new Path(workOutputPath, filePath);
            return new OutputStreamWriter(path.getFileSystem(context.getConfiguration()).create(path));
        }
    }
}
