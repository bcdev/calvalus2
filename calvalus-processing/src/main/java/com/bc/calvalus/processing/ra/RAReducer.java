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

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import static org.hsqldb.HsqlDateTime.e;

/**
 * The reducer for the region analysis workflow
 *
 * @author MarcoZ
 */
public class RAReducer extends Reducer<RAKey, RAValue, NullWritable, NullWritable> {

    private RAConfig raConfig;
    private long[][] dateRanges;
    private DateFormat dateFormat;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.raConfig = RAConfig.get(conf);
        String dateRangesString = conf.get(JobConfigNames.CALVALUS_RA_DATE_RANGES);
        dateRanges = createDateRanges(dateRangesString);
        dateFormat = createDateFormat();
    }

    private static DateFormat createDateFormat() {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        final Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ENGLISH);
        dateFormat.setCalendar(calendar);
        return dateFormat;
    }

    // key == region
    // values == ordered by time, extracts
    @Override
    protected void reduce(RAKey key, Iterable<RAValue> values, Context context) throws IOException, InterruptedException {
        // write netcdf file
        // compute stats
        // write CSV

        int regionId = key.getRegionId();
        String regionName = raConfig.getRegions()[regionId].getName();
        System.out.println("regionName = " + regionName);

        // open netcdf
        // NFileWriteable nFileWriteable = N4FileWriteable.create("region " + regionId);
        Stat stat = null;
        int currentTimeRangeIndex = -1;
        boolean headerWritten = false;

        for (RAValue extract : values) {
            long time = extract.getTime();
            System.out.println("time = " + time);
            System.out.println("numSamples = " + extract.getSamples().length);
            int trIndex = findDateRangeIndex(time);
            if (trIndex == -1) {
                String out_ouf_range_date = dateFormat.format(new Date(time));
                System.out.println("out_ouf_range_date = " + out_ouf_range_date);
            } else {
                if (trIndex != currentTimeRangeIndex) {
                    if (stat != null) {
                        if (!headerWritten) {
                            System.out.println(String.join(",", stat.getHeader()));
                            headerWritten = true;
                        }
                        stat.finalize();
                        System.out.println(String.join(",", stat.getStats()));
                    }
                    long[] dateRange = dateRanges[trIndex];
                    String dStart = dateFormat.format(new Date(dateRange[0]));
                    String dEnd = dateFormat.format(new Date(dateRange[1]));
                    stat = new Stat(regionName, raConfig.getBandNames(), dStart, dEnd);
                    currentTimeRangeIndex = trIndex;
                }
                stat.accumulate(extract);
            }
        }
        if (stat != null) {
            if (!headerWritten) {
                System.out.println(String.join(",", stat.getHeader()));
            }
            stat.finalize();
            System.out.println(String.join(",", stat.getStats()));
        }
        // close netcdf
    }

    private int findDateRangeIndex(long time) {
        for (int i = 0; i < dateRanges.length; i++) {
            long[] dateRange = dateRanges[i];
            if (time > dateRange[0] && time <= dateRange[1]) {
                return i;
            }
        }
        return -1;
    }

    private static long[][] createDateRanges(String dateRangesString) throws IOException {
        String[] dateRangesStrings = dateRangesString.split(",");
        long[][] dateRanges = new long[dateRangesStrings.length][2];
        for (int i = 0; i < dateRangesStrings.length; i++) {
            try {
                String dateRangeString = dateRangesStrings[i];
                DateRange dateRange = DateRange.parseDateRange(dateRangeString);
                dateRanges[i][0] = dateRange.getStartDate().getTime();
                dateRanges[i][1] = dateRange.getStopDate().getTime() + 24 * 60 *60 *1000L;
            } catch (ParseException e) {
                throw new IOException(e);
            }
        }
        return dateRanges;
    }

    private static class Stat {
        private final BandStat[] bandStats;
        private final String dStart;
        private final String dEnd;
        private final String regionName;
        int numPasses;
        int numObs;

        public Stat(String regionName, String[] bandNames, String dStart, String dEnd) {
            this.regionName = regionName;
            this.bandStats = new BandStat[bandNames.length];
            this.dStart = dStart;
            this.dEnd = dEnd;
            for (int i = 0; i < bandNames.length; i++) {
                this.bandStats[i] = new BandStat(bandNames[i]);
            }
            this.numObs = 0;
            this.numPasses = 0;
        }

        public void accumulate(RAValue extract) {
            numPasses++;
            numObs += extract.getNumPixel();
            float[][] samples = extract.getSamples();
            for (int bandId = 0; bandId < samples.length; bandId++) {
                bandStats[bandId].accumulate(samples[bandId]);
            }
        }

        public void finalize() {
            for (BandStat bandStat : bandStats) {
                bandStat.finalize();
            }
        }

        public List<String> getHeader() {
            List<String> header = new ArrayList<>();
            header.add("RegionId");
            header.add("TimeWindow_start");
            header.add("TimeWindow_end");
            header.add("numPasses");
            header.add("numObs");
            for (BandStat bandStat : bandStats) {
                header.addAll(bandStat.getHeader());
            }
            return header;
        }

        public List<String> getStats() {
            List<String> stats = new ArrayList<>();
            stats.add(regionName);
            stats.add(dStart);
            stats.add(dEnd);
            stats.add(Integer.toString(numPasses));
            stats.add(Integer.toString(numObs));
            for (BandStat bandStat : bandStats) {
                stats.addAll(bandStat.getStats());
            }
            return stats;
        }
    }

    private static class BandStat {

        private final String bandname;
        private int numValid = 0;
        private double min = +Double.MAX_VALUE;
        private double max = -Double.MAX_VALUE;
        private double sum = 0;
        private double sumSQ = 0;
        private double mean = Double.NaN;
        private double sigma = Double.NaN;

        public BandStat(String bandname) {
            this.bandname = bandname;
        }

        public void accumulate(float[] samples) {
            for (float sample : samples) {
                if (!Double.isNaN(sample)) {
                    numValid++;
                    min = Math.min(min, sample);
                    max = Math.max(max, sample);
                    sum += sample;
                    sumSQ += sample * sample;
                }
            }
        }

        public void finalize() {
            if (numValid > 0) {
                mean = sum / numValid;
                double sigmaSqr = sumSQ / numValid - mean * mean;
                sigma = sigmaSqr > 0.0 ? Math.sqrt(sigmaSqr) : 0.0;
            }
        }

        public List<String> getHeader() {
            List<String> header = new ArrayList<>();
            header.add(bandname + "_count");
            header.add(bandname + "_min");
            header.add(bandname + "_max");
            header.add(bandname + "_mean");
            header.add(bandname + "_sigma");
            return header;
        }

        public List<String> getStats() {
            List<String> stats = new ArrayList<>();
            stats.add(Integer.toString(numValid));
            stats.add(Double.toString(min));
            stats.add(Double.toString(max));
            stats.add(Double.toString(mean));
            stats.add(Double.toString(sigma));
            return stats;
        }
    }
}
