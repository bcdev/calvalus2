/*
 * Copyright (C) 2026 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 */

package com.bc.calvalus.processing.l3;

import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.TemporalBinSource;
import org.esa.snap.binning.support.SEAGrid;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Opt-in end-to-end writer benchmark. The source is generated lazily so the
 * measurement also verifies that a global grid does not require all bins in memory.
 */
public class SeaGridNetcdfFormatterBenchmarkTest {

    @Test
    public void benchmarksConfiguredGlobalGrid() throws Exception {
        Assume.assumeTrue("Enable with -Dcalvalus.test.seagridBenchmark=true.",
                          Boolean.getBoolean("calvalus.test.seagridBenchmark"));

        int numRows = positiveIntProperty("calvalus.benchmark.numRows", 1440);
        int featureCount = positiveIntProperty("calvalus.benchmark.featureCount", 4);
        int binStride = positiveIntProperty("calvalus.benchmark.binStride", 32);
        SEAGrid grid = new SEAGrid(numRows);
        long totalBins = grid.getNumBins();
        long populatedBins = (totalBins + binStride - 1) / binStride;
        String configuredOutput = System.getProperty("calvalus.benchmark.output");
        boolean retainOutput = configuredOutput != null && !configuredOutput.trim().isEmpty();
        File outputFile = retainOutput
                          ? new File(configuredOutput)
                          : File.createTempFile("calvalus-seagrid-benchmark-" + numRows + "-", ".nc");
        GeneratedSource source = new GeneratedSource(totalBins, featureCount, binStride);

        try {
            long startedNanos = System.nanoTime();
            SeaGridNetcdfFormatter.write(outputFile,
                                         grid,
                                         source,
                                         featureNames(featureCount),
                                         null);
            double elapsedSeconds = (System.nanoTime() - startedNanos) / 1.0e9;
            long outputBytes = outputFile.length();
            double totalBinsPerSecond = totalBins / elapsedSeconds;

            List<String> problems = SeaGridNetcdfValidator.validate(outputFile, numRows);
            assertTrue(problems.toString(), problems.isEmpty());
            assertEquals(1, source.processedPartCount);
            assertTrue(source.closed);
            assertEquals(populatedBins, source.generatedBinCount);
            assertTrue("Benchmark output must not be empty.", outputBytes > 0);

            System.out.printf(Locale.ENGLISH,
                              "SEAGRID_BENCHMARK numRows=%d totalBins=%d populatedBins=%d " +
                              "featureCount=%d stride=%d elapsedSeconds=%.3f outputBytes=%d " +
                              "totalBinsPerSecond=%.0f%n",
                              numRows, totalBins, populatedBins, featureCount, binStride,
                              elapsedSeconds, outputBytes, totalBinsPerSecond);
        } finally {
            if (!retainOutput && outputFile.exists() && !outputFile.delete()) {
                outputFile.deleteOnExit();
            }
        }
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        String text = System.getProperty(name);
        int value = text == null ? defaultValue : Integer.parseInt(text);
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
        return value;
    }

    private static String[] featureNames(int featureCount) {
        String[] names = new String[featureCount];
        for (int index = 0; index < names.length; index++) {
            names[index] = "science_" + index;
        }
        return names;
    }

    private static final class GeneratedSource implements TemporalBinSource {

        private final long totalBins;
        private final int featureCount;
        private final int stride;
        private long generatedBinCount;
        private int processedPartCount;
        private boolean closed;

        private GeneratedSource(long totalBins, int featureCount, int stride) {
            this.totalBins = totalBins;
            this.featureCount = featureCount;
            this.stride = stride;
        }

        @Override
        public int open() {
            return 1;
        }

        @Override
        public Iterator<? extends TemporalBin> getPart(int index) {
            if (index != 0) {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            return new Iterator<TemporalBin>() {
                private long binIndex;

                @Override
                public boolean hasNext() {
                    return binIndex < totalBins;
                }

                @Override
                public TemporalBin next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    TemporalBin bin = new TemporalBin(binIndex, featureCount);
                    float[] values = bin.getFeatureValues();
                    for (int featureIndex = 0; featureIndex < values.length; featureIndex++) {
                        values[featureIndex] = (float) (binIndex % 1000) + featureIndex;
                    }
                    generatedBinCount++;
                    binIndex += stride;
                    return bin;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) {
            processedPartCount++;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
