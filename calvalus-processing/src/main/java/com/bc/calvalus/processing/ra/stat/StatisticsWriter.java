/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.processing.ra.RAConfig;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes statistics, depending on the configuration to different files.
 */
class StatisticsWriter {

    private final boolean statisticsPerRegion;
    private final boolean separateHistogram;
    private final Statistics[] stats;

    private final Writer[][] writer;

    StatisticsWriter(RAConfig raConfig, Statistics[] stats, WriterFactory writerFactory) throws IOException {
        this.statisticsPerRegion = raConfig.isWriteStatisticsFilePerRegion();
        this.separateHistogram = raConfig.isWriteSeparateHistogramFile();
        this.stats = stats;
        RAConfig.BandConfig[] bandConfigs = raConfig.getBandConfigs();
        String[] bandNames = new String[bandConfigs.length];
        for (int i = 0; i < bandConfigs.length; i++) {
            RAConfig.BandConfig bConfig = bandConfigs[i];
            bandNames[i] = bConfig.getName();
        }
        int numWriter = 1;
        if (separateHistogram) {
            numWriter = 1 + bandConfigs.length;
        }
        if (statisticsPerRegion) {
            writer = new Writer[raConfig.getInternalRegionNames().length][numWriter];
            String[] internalRegionNames = raConfig.getInternalRegionNames();
            for (int r = 0; r < internalRegionNames.length; r++) {
                String regionName = internalRegionNames[r];

                writer[r][0] = writerFactory.createWriter("region-" + regionName + "-statistics.csv");
                writeLine(writer[r][0], getHeader(bandNames, true));

                if (separateHistogram) {
                    for (int b = 0; b < bandConfigs.length; b++) {
                        if (bandConfigs[b].getNumBins() > 0) {
                            writer[r][1 + b] = writerFactory.createWriter("region-" + regionName + "-histogram-" + bandNames[b] + ".csv");
                            writeLine(writer[r][1 + b], getHeader(bandNames, false));
                        }
                    }
                }
            }
        } else {
            writer = new Writer[1][numWriter];

            writer[0][0] = writerFactory.createWriter("region-statistics.csv");
            writeLine(writer[0][0], getHeader(bandNames, true));

            if (separateHistogram) {
                for (int b = 0; b < bandConfigs.length; b++) {
                    if (bandConfigs[b].getNumBins() > 0) {
                        writer[0][1 + b] = writerFactory.createWriter("region-histogram-" + bandNames[b] + ".csv");
                        writeLine(writer[0][1 + b], getHeader(bandNames, false));
                    }
                }
            }
        }
    }

    void writeRecord(int region, List<String> commonStats) throws IOException {

        List<String> records = new ArrayList<>();
        records.addAll(commonStats);
        for (Statistics stat : stats) {
            records.addAll(stat.getStatisticsRecords());
        }

        Writer[] writersPerRegion = writer[0];
        if (statisticsPerRegion) {
            writersPerRegion = writer[region];
        }

        if (separateHistogram) {
            writeLine(writersPerRegion[0], records);

            for (int bandIndex = 0; bandIndex < this.stats.length; bandIndex++) {
                // TODO test stat with and without histo

                if (writersPerRegion[1 + bandIndex] != null) {
                    records = new ArrayList<>(commonStats);
                    records.addAll(stats[bandIndex].getHistogramRecords());
                    writeLine(writersPerRegion[1 + bandIndex], records);
                }
            }
        } else {
            for (int bandIndex = 0; bandIndex < this.stats.length; bandIndex++) {
                // TODO test stat with and without histo
                records.addAll(stats[bandIndex].getHistogramRecords());
            }
            writeLine(writersPerRegion[0], records);
        }
    }

    void close() throws IOException {
        for (Writer[] writerPerRegion : writer) {
            for (Writer w : writerPerRegion) {
                w.close();
            }
        }
    }

    private List<String> getHeader(String[] bandNames, boolean stat) {
        List<String> records = new ArrayList<>();
        records.add("RegionId");
        records.add("TimeWindow_start");
        records.add("TimeWindow_end");
        records.add("numPasses");
        records.add("numObs");
        if (separateHistogram) {
            for (int bandIndex = 0; bandIndex < bandNames.length; bandIndex++) {
                if (stat) {
                    records.addAll(stats[bandIndex].getStatisticsHeaders(bandNames[bandIndex]));
                } else {
                    records.addAll(stats[bandIndex].getHistogramHeaders(bandNames[bandIndex]));
                }
            }
        } else {
            for (int bandIndex = 0; bandIndex < bandNames.length; bandIndex++) {
                records.addAll(stats[bandIndex].getStatisticsHeaders(bandNames[bandIndex]));
            }
            for (int bandIndex = 0; bandIndex < bandNames.length; bandIndex++) {
                records.addAll(stats[bandIndex].getHistogramHeaders(bandNames[bandIndex]));
            }
        }
        return records;
    }

    private static void writeLine(Writer writer, List<String> fields) throws IOException {
        writer.write(String.join("\t", fields) + "\n");
    }

}
