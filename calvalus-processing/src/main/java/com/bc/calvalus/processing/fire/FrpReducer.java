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

package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.l3.L3Reducer;
import com.bc.calvalus.processing.l3.L3SpatialBin;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Logger;

/**
 * Merges stream of spatial bins with FRP pixels into one of the aggregation outputs
 *
 * @author boe
 */
public class FrpReducer extends L3Reducer {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static long THIRTY_YEARS;
    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            THIRTY_YEARS = ISO_DATE_FORMAT.parse("2000-01-01T00:00:00.000Z").getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        if ("l2monthly".equals(context.getConfiguration().get("calvalus.targetFormat", "l2monthly"))) {
            try (BufferedWriter out = new BufferedWriter(new FileWriter(new File("somename")))) {
                out.write("Time\tLatitude\tLongitude\tRow\tColumn\tFRP_MIR\tFRP_SWIR\tAREA\tday_flag\tf1_flag\tPlatform\tConfidence\n");
                while (context.nextKey()) {
                    LongWritable binIndex = (LongWritable) context.getCurrentKey();
                    for (L3SpatialBin bin : context.getValues()) {
                        out.write(ISO_DATE_FORMAT.format(new Date(binIndex.get() / 1000 + THIRTY_YEARS)));
                        out.write('\t');
                        out.write(String.format("%8.5f", bin.getFeatureValues()[1]));
                        out.write('\t');
                        out.write(String.format("%8.5f", bin.getFeatureValues()[2]));
                        out.write('\t');
                        out.write(String.format("%d", (int) bin.getFeatureValues()[3]));
                        out.write('\t');
                        out.write(String.format("%d", (int) bin.getFeatureValues()[4]));
                        out.write('\t');
                        out.write(String.format("%f", bin.getFeatureValues()[5]));
                        out.write('\t');
                        out.write(String.format("%f", bin.getFeatureValues()[6]));
                        out.write('\t');
                        out.write(String.format("%f", bin.getFeatureValues()[7]));
                        out.write('\t');
                        out.write(String.format("%d", (int) bin.getFeatureValues()[8]));
                        out.write('\t');
                        out.write(String.format("%d", (int) bin.getFeatureValues()[9]));
                        out.write('\t');
                        out.write(String.format("%s", bin.getFeatureValues()[0] == 1 ? "S3A" : bin.getFeatureValues()[0] == 2 ? "S3B" : "unknown"));
                        out.write('\t');
                        out.write(String.format("%f", bin.getFeatureValues()[10]));
                        out.write('\n');
                    }
                }
            }
            LOG.info("Copying file to HDFS");
            Path workPath = new Path(FileOutputFormat.getWorkOutputPath(context), "somename");
            try (InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("somename")))) {
                try (OutputStream outputStream = workPath.getFileSystem(context.getConfiguration()).create(workPath)) {
                    byte[] buffer = new byte[64 * 1024];
                    while (true) {
                        int n = inputStream.read(buffer);
                        if (n <= 0) break;
                        outputStream.write(buffer, 0, n);
                    }
                }
            }
        } else {
            super.run(context);
        }
    }

}
