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

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import static com.bc.calvalus.commons.DateUtils.createCalendar;
import static com.bc.calvalus.processing.fire.FrpMapper.isDay;
import static com.bc.calvalus.processing.fire.FrpMapper.isWater;

/**
 * Merges stream of spatial bins with FRP pixels into one of the aggregation outputs
 *
 * @author boe
 */
public class FrpReducer extends L3Reducer {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private static final int PLATFORM_IDX = 0;
    private static final int LAT_IDX = 1;
    private static final int LON_IDX = 2;
    private static final int ROW_IDX = 3;
    private static final int COL_IDX = 4;
    private static final int FRP_MIR_IDX = 5;
    private static final int FRP_MIR_UNC_IDX = 6;
    private static final int FRP_SWIR_IDX = 7;
    private static final int FRP_SWIR_UNC_IDX = 8;
    private static final int AREA_IDX = 9;
    private static final int FLAGS_IDX = 10;
    private static final int F1_FLAG_IDX = 11;
    private static final int CLASSIFICATION_IDX = 12;
    private static final int CONF_IDX = 13;
    private static final int SAT_ZENITH_IDX = 14;

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

    static String[] getDateTime(Date date, Calendar calendar) {
        calendar.setTime(date);
        final int year = calendar.get(Calendar.YEAR);
        final int month = calendar.get(Calendar.MONTH) + 1;
        final int day = calendar.get(Calendar.DAY_OF_MONTH);

        final int hour = calendar.get(Calendar.HOUR_OF_DAY);
        final int minute = calendar.get(Calendar.MINUTE);
        final int second = calendar.get(Calendar.SECOND);

        final String[] result = new String[2];
        result[0] = String.format(Locale.ENGLISH, "%04d%02d%02d", year, month, day);
        result[1] = String.format(Locale.ENGLISH, "%02d%02d%02d", hour, minute, second);
        return result;
    }

    static void writeL2CSV(Context context, GregorianCalendar utcCalendar, Writer out) throws IOException, InterruptedException {
        out.write("Column\tRow\tDate\tTime\tLatitude\tLongitude\tsat_zenith\tFRP_MWIR\tFRP_MWIR_uncertainty\tFRP_SWIR\tFRP_SWIR_uncertainty\tConfidence\tF1_flag\tDay_flag\tArea\tPlatform\tLand/Ocean\tHotspot_class\n");
        while (context.nextKey()) {
            final LongWritable binIndex = context.getCurrentKey();
            for (L3SpatialBin bin : context.getValues()) {
                final Date date = new Date(binIndex.get() / 1000 + THIRTY_YEARS);
                final int flags = (int) bin.getFeatureValues()[FLAGS_IDX];

                // Column
                // Row
                // Date
                // Time
                // Latitude
                // Longitude
                // sat_zenith
                // FRP_MWIR
                // FRP_MWIR_uncertainty
                // FRP_SWIR
                // FRP_SWIR_uncertainty
                // Confidence
                // F1_flag
                // Day_flag
                // Area
                // Platform
                // Land/Ocean
                // Hotspot_class

                out.write(String.format(Locale.ENGLISH, "%d", (int) bin.getFeatureValues()[COL_IDX]));
                out.write('\t');
                out.write(String.format(Locale.ENGLISH, "%d", (int) bin.getFeatureValues()[ROW_IDX]));
                out.write('\t');

                final String[] dateTime = getDateTime(date, utcCalendar);
                out.write(dateTime[0]);
                out.write('\t');
                out.write(dateTime[1]);
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%3.5f", bin.getFeatureValues()[LAT_IDX]));
                out.write('\t');
                out.write(String.format(Locale.ENGLISH, "%3.5f", bin.getFeatureValues()[LON_IDX]));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%3.5f", bin.getFeatureValues()[SAT_ZENITH_IDX]));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%f", bin.getFeatureValues()[FRP_MIR_IDX]));
                out.write('\t');
                out.write(String.format(Locale.ENGLISH, "%f", bin.getFeatureValues()[FRP_MIR_UNC_IDX]));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%f", bin.getFeatureValues()[FRP_SWIR_IDX]));
                out.write('\t');
                out.write(String.format(Locale.ENGLISH, "%f", bin.getFeatureValues()[FRP_SWIR_UNC_IDX]));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%f", bin.getFeatureValues()[CONF_IDX]));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%d", (int) bin.getFeatureValues()[F1_FLAG_IDX]));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%d", isDay(flags) ? 1 : 0));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%f", bin.getFeatureValues()[AREA_IDX]));
                out.write('\t');

                final int platformNumber = (int) bin.getFeatureValues()[PLATFORM_IDX];
                out.write(String.format("%s", platformNumber == 1 ? "S3A" : platformNumber == 2 ? "S3B" : "unknown"));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%d", isWater(flags) ? 0 : 1));
                out.write('\t');

                out.write(String.format(Locale.ENGLISH, "%d", (int) bin.getFeatureValues()[CLASSIFICATION_IDX]));

                out.write('\n');
            }
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        if ("l2monthly".equals(context.getConfiguration().get("calvalus.targetFormat", "l2monthly"))) {
            final GregorianCalendar utcCalendar = createCalendar();
            // 20200801-C3S-L2-FRP-SLSTR-P1M-fv0.4.csv
            final String minDate = context.getConfiguration().get("calvalus.minDate");
            final String version = context.getConfiguration().get("calvalus.output.version");
            final String fileName = String.format("%s%s%s-C3S-L2-FRP-SLSTR-P1M-fv%s.csv", minDate.substring(0,4), minDate.substring(5,7), minDate.substring(8,10), version);
            try (BufferedWriter out = new BufferedWriter(new FileWriter(new File(fileName)))) {
                writeL2CSV(context, utcCalendar, out);
            }
            LOG.info("Copying file to HDFS");
            final Path workPath = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);
            try (InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(fileName)))) {
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
