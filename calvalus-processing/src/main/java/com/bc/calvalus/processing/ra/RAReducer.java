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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.ParseException;
import java.util.*;

/**
 * The reducer for the region analysis workflow
 *
 * @author MarcoZ
 */
public class RAReducer extends Reducer<RAKey, RAValue, NullWritable, NullWritable> {

    private RAConfig raConfig;
    private RADateRanges dateRanges;
    private Writer writer;
    private boolean headerWritten = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.raConfig = RAConfig.get(conf);
        String dateRangesString = conf.get(JobConfigNames.CALVALUS_RA_DATE_RANGES);
        try {
            dateRanges = RADateRanges.create(dateRangesString);
        } catch (ParseException e) {
            throw new IOException(e);
        }

        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), "region-analysis.csv");
        writer = new OutputStreamWriter(path.getFileSystem(context.getConfiguration()).create(path));
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        writer.close();
    }

    // key == region
    // values == extracts, ordered by time
    @Override
    protected void reduce(RAKey key, Iterable<RAValue> values, Context context) throws IOException, InterruptedException {
        // 1) write netCDF file
        // 2) compute stats
        // 3) write CSV

        final int regionId = key.getRegionId();
        final String regionName = raConfig.getRegions()[regionId].getName();
        System.out.println("regionName = " + regionName);

        // open netCDF
        // NFileWriteable nFileWriteable = N4FileWriteable.create("region " + regionId);
        Stat stat = null;
        int lastTimeRange = -1;

        for (RAValue extract : values) {
            long time = extract.getTime();
            System.out.println("time = " + time + "  numSamples = " + extract.getSamples()[0].length);
            int currentTimeRange = dateRanges.findIndex(time);
            if (currentTimeRange == -1) {
                String out_ouf_range_date = dateRanges.format(time);
                System.out.println("out_ouf_range_date = " + out_ouf_range_date + " --> ignoring extract data");
            } else {
                if (currentTimeRange != lastTimeRange) {
                    // next/new time-range
                    if (stat != null) {
                        stat.write(writer);
                    }
                    String dStart = dateRanges.formatStart(currentTimeRange);
                    String dEnd = dateRanges.formatEnd(currentTimeRange);
                    stat = new Stat(regionName, raConfig.getBandNames(), dStart, dEnd);
                    lastTimeRange = currentTimeRange;
                }
                stat.accumulate(extract);
            }
        }
        if (stat != null) {
            stat.write(writer);
        }
        // close netCDF
    }

    private void write(List<String> strings) throws IOException {
    }

    private static class Stat {

        private final Accumulator[] accus;
        private final String dStart;
        private final String dEnd;
        private final String regionName;

        private Stx[] bandStats;
        int numPasses;
        int numObs;
        boolean headerWritten;

        public Stat(String regionName, String[] bandNames, String dStart, String dEnd) {
            this.regionName = regionName;
            this.accus = new Accumulator[bandNames.length];
            this.dStart = dStart;
            this.dEnd = dEnd;
            for (int i = 0; i < bandNames.length; i++) {
                this.accus[i] = new Accumulator(bandNames[i].trim());
            }
            this.numObs = 0;
            this.numPasses = 0;
        }

        public void accumulate(RAValue extract) {
            numPasses++;
            numObs += extract.getNumPixel();
            float[][] samples = extract.getSamples();
            for (int bandId = 0; bandId < samples.length; bandId++) {
                accus[bandId].accumulate(samples[bandId]);
            }
        }

        public void finish() {
            bandStats = new Stx[accus.length];
            for (int i = 0; i < accus.length; i++) {
                bandStats[i] = new Stx(accus[i].getBandname(), accus[i].getValues());
            }
        }

        public List<String> getHeader() {
            List<String> header = new ArrayList<>();
            header.add("RegionId");
            header.add("TimeWindow_start");
            header.add("TimeWindow_end");
            header.add("numPasses");
            header.add("numObs");
            for (Stx bandStat : bandStats) {
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
            for (Stx bandStat : bandStats) {
                stats.addAll(bandStat.getStats());
            }
            return stats;
        }

        public void write(Writer writer) throws IOException {
            if (!headerWritten) {
                writeLine(writer, getHeader());
                headerWritten = true;
            }
            finish();
            writeLine(writer, getStats());
        }

        private static void writeLine(Writer writer, List<String> fields) throws IOException {
            writer.write(String.join("\t", fields) + "\n");
        }
    }

    static class Accumulator {

        private final String bandname;
        private float[] values = new float[0];

        public Accumulator(String bandname) {
            this.bandname = bandname;
        }

        public void accumulate(float... samples) {
            float[] buffer = new float[samples.length];
            int i = 0;
            for (float sample : samples) {
                if (!Float.isNaN(sample)) {
                    buffer[i++] = sample;
                }
            }
            values = concat(values, buffer);
        }

        public String getBandname() {
            return bandname;
        }

        public float[] getValues() {
            return values;
        }

        static float[] concat(float[] a1, float[] a2) {
            float[] b = new float[a1.length + a2.length];
            System.arraycopy(a1, 0, b, 0, a1.length);
            System.arraycopy(a2, 0, b, a1.length, a2.length);
            return b;
        }
    }

    static class Stx {

        private final String bandname;
        private final int numValid;
        private double min = +Double.MAX_VALUE;
        private double max = -Double.MAX_VALUE;
        private final double arithMean;
        private final double sigma;
        private final double geomMean;
        private final double p5;
        private final double p25;
        private final double p50;
        private final double p75;
        private final double p95;
//        private final double mode; //TODO

        public Stx(String name, float[] values) {
            this.bandname = name;
            this.numValid = values.length;
            if (numValid > 0) {
                double sum = 0;
                double sumSQ = 0;
                double product = 1;
                boolean geomMeanValid = true;
                for (float value : values) {
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                    sum += value;
                    sumSQ += value * value;
                    if (geomMeanValid) {
                        if (value > 0) {
                            product *= value;
                        } else {
                            geomMeanValid = false;
                        }
                    }
                }
                arithMean = sum / numValid;
                double sigmaSqr = sumSQ / numValid - arithMean * arithMean;
                sigma = sigmaSqr > 0.0 ? Math.sqrt(sigmaSqr) : 0.0;
                geomMean = geomMeanValid ? Math.pow(product, 1.0 / numValid) : Double.NaN;
                Arrays.sort(values);
                p5 = computePercentile(5, values);
                p25 = computePercentile(25, values);
                p50 = computePercentile(50, values);
                p75 = computePercentile(75, values);
                p95 = computePercentile(95, values);
            } else {
                arithMean = Double.NaN;
                sigma = Double.NaN;
                geomMean = Double.NaN;
                p5 = Double.NaN;
                p25 = Double.NaN;
                p50 = Double.NaN;
                p75 = Double.NaN;
                p95 = Double.NaN;
            }
        }

        /**
         * Computes the p-th percentile of an array of measurements following
         * the "Engineering Statistics Handbook: Percentile". NIST.
         * http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm.
         * Retrieved 2011-03-16.
         *
         * @param p            The percentage in percent ranging from 0 to 100.
         * @param measurements Sorted array of measurements.
         * @return The  p-th percentile.
         */
        static double computePercentile(int p, float[] measurements) {
            int N = measurements.length;
            double n = (p / 100.0) * (N + 1);
            int k = (int) Math.floor(n);
            double d = n - k;
            double yp;
            if (k == 0) {
                yp = measurements[0];
            } else if (k >= N) {
                yp = measurements[N - 1];
            } else {
                yp = measurements[k - 1] + d * (measurements[k] - measurements[k - 1]);
            }
            return yp;
        }

        public List<String> getHeader() {
            List<String> header = new ArrayList<>();
            header.add(bandname + "_count");
            header.add(bandname + "_min");
            header.add(bandname + "_max");
            header.add(bandname + "_arithMean");
            header.add(bandname + "_sigma");
            header.add(bandname + "_geomMean");
            return header;
        }

        public List<String> getStats() {
            List<String> stats = new ArrayList<>();
            stats.add(Integer.toString(numValid));
            stats.add(Double.toString(min));
            stats.add(Double.toString(max));
            stats.add(Double.toString(arithMean));
            stats.add(Double.toString(sigma));
            stats.add(Double.toString(geomMean));
            return stats;
        }
    }
}
