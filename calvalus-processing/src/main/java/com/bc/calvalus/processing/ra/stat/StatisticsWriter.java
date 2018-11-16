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
import java.util.NoSuchElementException;

/**
 * Writes statistics, depending on the configuration to different files.
 */
class StatisticsWriter {

    private final boolean statisticsPerRegion;
    private final boolean separateHistogram;
    private final Statistics[] stats;

    private final WriterFactory writerFactory;
    private final Writer[][] writer;
    private final String[] bandNames;
    private final RAConfig.BandConfig[] bandConfigs;
    private final String[] internalRegionNames;
    private final boolean withProductNames;

    StatisticsWriter(RAConfig raConfig, Statistics[] stats, WriterFactory writerFactory) throws IOException {
        this.statisticsPerRegion = raConfig.isWritePerRegion();
        this.separateHistogram = raConfig.isWriteSeparateHistogram();
        this.stats = stats;
        this.bandNames = raConfig.getBandNames();
        this.bandConfigs = raConfig.getBandConfigs();
        this.writerFactory = writerFactory;
        internalRegionNames = raConfig.getInternalRegionNames();
        int numWriter = 1;
        withProductNames = raConfig.withProductNames();

        if (separateHistogram) {
            numWriter = 1 + bandNames.length;
        }
        if (statisticsPerRegion) {
            writer = new Writer[internalRegionNames.length][numWriter];
        } else {
            writer = new Writer[1][numWriter];
        }
//        List<String> commonHeader = getCommonHeader();
//        if (statisticsPerRegion) {
//            writer = new Writer[raConfig.getInternalRegionNames().length][numWriter];
//            String[] internalRegionNames = raConfig.getInternalRegionNames();
//            for (int r = 0; r < internalRegionNames.length; r++) {
//                String regionName = internalRegionNames[r];
//                if (separateHistogram) {
//                    // stat
//                    List<String> records = new ArrayList<>(commonHeader);
//                    records.addAll(getStatHeader(bandNames));
//                    writer[r][0] = writerFactory.createWriter("region-" + regionName + "-statistics.csv");
//                    writeLine(writer[r][0], records);
//
//                    for (int b = 0; b < bandConfigs.length; b++) {
//                        if (bandConfigs[b].getNumBins() > 0) {
//                            // histo
//                            records = new ArrayList<>(commonHeader);
//                            records.addAll(getHistoHeader(b, bandNames[b]));
//
//                            writer[r][1 + b] = writerFactory.createWriter("region-" + regionName + "-histogram-" + bandNames[b] + ".csv");
//                            writeLine(writer[r][1 + b], records);
//                        }
//                    }
//                } else {
//                    // stat + histo
//                    List<String> records = new ArrayList<>(commonHeader);
//                    records.addAll(getStatAndHistoHeader(bandNames));
//                    writer[r][0] = writerFactory.createWriter("region-" + regionName + "-statistics.csv");
//                    writeLine(writer[r][0], records);
//                }
//            }
//        } else {
//            writer = new Writer[1][numWriter];
//            if (separateHistogram) {
//                // stat
//                List<String> records = new ArrayList<>(commonHeader);
//                records.addAll(getStatHeader(bandNames));
//                writer[0][0] = writerFactory.createWriter("region-statistics.csv");
//                writeLine(writer[0][0], records);
//
//                for (int b = 0; b < bandConfigs.length; b++) {
//                    if (bandConfigs[b].getNumBins() > 0) {
//                        // histo
//                        records = new ArrayList<>(commonHeader);
//                        records.addAll(getHistoHeader(b, bandNames[b]));
//                        writer[0][1 + b] = writerFactory.createWriter("region-histogram-" + bandNames[b] + ".csv");
//                        writeLine(writer[0][1 + b], records);
//                    }
//                }
//            } else {
//                // stat + histo
//                List<String> records = new ArrayList<>(commonHeader);
//                records.addAll(getStatAndHistoHeader(bandNames));
//                writer[0][0] = writerFactory.createWriter("region-statistics.csv");
//                writeLine(writer[0][0], records);
//            }
//        }
    }

    void createWriters(String regionName) throws IOException {
        List<String> commonHeader = getCommonHeader();
        if (statisticsPerRegion && separateHistogram) {
            int r = regionIndexOf(regionName);
            // stat
            List<String> records = new ArrayList<>(commonHeader);
            records.addAll(getStatHeader(bandNames));
            writer[r][0] = writerFactory.createWriter("region-" + regionName + "-statistics.csv");
            writeLine(writer[r][0], records);
            for (int b = 0; b < bandConfigs.length; b++) {
                if (bandConfigs[b].getNumBins() > 0) {
                    // histo
                    records = new ArrayList<>(commonHeader);
                    records.addAll(getHistoHeader(b, bandNames[b]));
                    writer[r][1 + b] = writerFactory.createWriter("region-" + regionName + "-histogram-" + bandNames[b] + ".csv");
                    writeLine(writer[r][1 + b], records);
                }
            }
        } else if (statisticsPerRegion) {
            int r = regionIndexOf(regionName);
            // stat + histo
            List<String> records = new ArrayList<>(commonHeader);
            records.addAll(getStatAndHistoHeader(bandNames));
            writer[r][0] = writerFactory.createWriter("region-" + regionName + "-statistics.csv");
            writeLine(writer[r][0], records);
        } else if (separateHistogram) {
            if (writer[0][0] == null) {
                // stat
                List<String> records = new ArrayList<>(commonHeader);
                records.addAll(getStatHeader(bandNames));
                writer[0][0] = writerFactory.createWriter("region-statistics.csv");
                writeLine(writer[0][0], records);
                for (int b = 0; b < bandConfigs.length; b++) {
                    if (bandConfigs[b].getNumBins() > 0) {
                        // histo
                        records = new ArrayList<>(commonHeader);
                        records.addAll(getHistoHeader(b, bandNames[b]));
                        writer[0][1 + b] = writerFactory.createWriter("region-histogram-" + bandNames[b] + ".csv");
                        writeLine(writer[0][1 + b], records);
                    }
                }
            }
        } else {
             if (writer[0][0] == null) {
                // stat + histo
                List<String> records = new ArrayList<>(commonHeader);
                records.addAll(getStatAndHistoHeader(bandNames));
                writer[0][0] = writerFactory.createWriter("region-statistics.csv");
                writeLine(writer[0][0], records);
             }
        }
    }

    void closeWriters(String regionName) throws IOException {
        if (statisticsPerRegion) {
            int r = regionIndexOf(regionName);
            for (int b = 0; b < writer[r].length; b++) {
                if (writer[r][b] != null) {
                    writer[r][b].close();
                    writer[r][b] = null;
                }
            }
        } else {
             // do nothing
        }
    }

    private int regionIndexOf(String regionName) {
        for (int r = 0; r < internalRegionNames.length; ++r) {
            if (internalRegionNames[r].equals(regionName)) {
                return r;
            }
        }
        throw new NoSuchElementException(regionName);
    }

    void writeRecord(int region, List<String> commonStats, String productName) throws IOException {
        Writer[] writersPerRegion = writer[0];
        if (statisticsPerRegion) {
            writersPerRegion = writer[region];
        }

        if (separateHistogram) {
            List<String> records = new ArrayList<>(commonStats);
            for (Statistics stat : stats) {
                records.addAll(stat.getStatisticsRecords());
            }
            writeLine(writersPerRegion[0], records);

            for (int bandIndex = 0; bandIndex < this.stats.length; bandIndex++) {
                if (writersPerRegion[1 + bandIndex] != null) {
                    records = new ArrayList<>(commonStats);
                    records.addAll(stats[bandIndex].getHistogramRecords());
                    writeLine(writersPerRegion[1 + bandIndex], records);
                }
            }
        } else {
            List<String> records = new ArrayList<>(commonStats);
            for (Statistics stat : this.stats) {
                records.addAll(stat.getStatisticsRecords());
                records.addAll(stat.getHistogramRecords());
                if (productName!=null) {
                    records.add(productName);
                }
            }
            writeLine(writersPerRegion[0], records);
        }
    }

    void close() throws IOException {
        for (Writer[] writerPerRegion : writer) {
            for (Writer w : writerPerRegion) {
                if (w != null) {
                    w.close();
                }
            }
        }
    }

    private List<String> getCommonHeader() {
        List<String> records = new ArrayList<>();
        records.add("RegionId");
        records.add("TimeWindow_start");
        records.add("TimeWindow_end");
        if (withProductNames) {
            records.add("Product");
        }
        records.add("numPasses");
        records.add("numObs");
        return records;
    }

    private List<String> getStatHeader(String[] bandNames) {
        List<String> records = new ArrayList<>();
        for (int bandIndex = 0; bandIndex < bandNames.length; bandIndex++) {
            records.addAll(stats[bandIndex].getStatisticsHeaders(bandNames[bandIndex]));
        }
        return records;
    }

    private List<String> getHistoHeader(int bandIndex, String bandName) {
        return stats[bandIndex].getHistogramHeaders(bandName);
    }

    private List<String> getStatAndHistoHeader(String[] bandNames) {
        List<String> records = new ArrayList<>();
        for (int bandIndex = 0; bandIndex < bandNames.length; bandIndex++) {
            records.addAll(stats[bandIndex].getStatisticsHeaders(bandNames[bandIndex]));
            records.addAll(stats[bandIndex].getHistogramHeaders(bandNames[bandIndex]));
        }
        return records;
    }

    private static void writeLine(Writer writer, List<String> fields) throws IOException {
        writer.write(String.join("\t", fields) + "\n");
    }

}
