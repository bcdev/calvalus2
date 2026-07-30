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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SeaGridNetcdfFormatterEdgeCasesTest {

    private File outputFile;

    @Before
    public void setUp() throws IOException {
        outputFile = File.createTempFile("calvalus-seagrid-edge-", ".nc");
    }

    @After
    public void tearDown() {
        if (outputFile != null && outputFile.exists() && !outputFile.delete()) {
            outputFile.deleteOnExit();
        }
    }

    @Test
    public void streamsMultiplePartsAcrossBufferSizedGap() throws Exception {
        SEAGrid grid = new SEAGrid(360);
        TrackingSource source = new TrackingSource(Arrays.asList(createBin(0, 1.0f, 2.0f)),
                                                   Arrays.asList(createBin(10000, 3.0f, 4.0f)));

        write(grid, source, new String[]{"first", "second"});

        assertEquals(2, source.processedPartCount);
        assertTrue(source.closed);
        NetcdfFile netcdfFile = NetcdfFile.open(outputFile.getAbsolutePath());
        try {
            Array first = netcdfFile.findVariable("first").read();
            Array second = netcdfFile.findVariable("second").read();
            int firstOutputIndex = outputIndex(grid, 0);
            int secondOutputIndex = outputIndex(grid, 10000);
            assertEquals(1.0f, first.getFloat(firstOutputIndex), 0.0f);
            assertTrue(Float.isNaN(first.getFloat(secondOutputIndex - 1)));
            assertEquals(3.0f, first.getFloat(secondOutputIndex), 0.0f);
            assertEquals(4.0f, second.getFloat(secondOutputIndex), 0.0f);
        } finally {
            netcdfFile.close();
        }
    }

    @Test
    public void rejectsUnorderedAndDuplicateBinsAndClosesSource() throws Exception {
        assertBadBins(new TemporalBin[]{createBin(5, 1.0f), createBin(4, 2.0f)},
                      "ordered by increasing global bin index");
        assertBadBins(new TemporalBin[]{createBin(5, 1.0f), createBin(5, 2.0f)},
                      "ordered by increasing global bin index");
    }

    @Test
    public void rejectsOutOfRangeIndexAndWrongFeatureCount() throws Exception {
        SEAGrid grid = new SEAGrid(4);
        assertBadBins(grid,
                      new TemporalBin[]{createBin(grid.getNumBins(), 1.0f)},
                      new String[]{"value"},
                      "outside the sinusoidal grid");
        assertBadBins(grid,
                      new TemporalBin[]{createBin(0, 1.0f)},
                      new String[]{"first", "second"},
                      "has 1 features; expected 2");
    }

    @Test
    public void rejectsReservedDuplicateAndEmptyFeatureNamesBeforeOpeningSource() throws Exception {
        assertBadNames(new String[]{"time"}, "Reserved NetCDF variable name");
        assertBadNames(new String[]{"value", "value"}, "Duplicate science-variable name");
        assertBadNames(new String[]{" "}, "must not be empty");
    }

    private void assertBadBins(TemporalBin[] bins, String expectedMessage) throws Exception {
        assertBadBins(new SEAGrid(4), bins, new String[]{"value"}, expectedMessage);
    }

    private void assertBadBins(SEAGrid grid,
                               TemporalBin[] bins,
                               String[] featureNames,
                               String expectedMessage) throws Exception {
        TrackingSource source = new TrackingSource(Arrays.asList(bins));
        try {
            write(grid, source, featureNames);
            fail("Expected IOException containing: " + expectedMessage);
        } catch (IOException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(expectedMessage));
        }
        assertTrue("Source must close when bin validation fails.", source.closed);
    }

    private void assertBadNames(String[] featureNames, String expectedMessage) throws Exception {
        TrackingSource source = new TrackingSource(Arrays.asList(createBin(0, 1.0f)));
        try {
            write(new SEAGrid(4), source, featureNames);
            fail("Expected IllegalArgumentException containing: " + expectedMessage);
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(expectedMessage));
        }
        assertTrue("Argument validation must happen before opening the source.", !source.opened);
    }

    private void write(SEAGrid grid, TrackingSource source, String[] featureNames) throws IOException {
        SeaGridNetcdfFormatter.write(outputFile,
                                     grid,
                                     source,
                                     featureNames,
                                     null,
                                     NetcdfFileWriter.Version.netcdf3);
    }

    private static TemporalBin createBin(long index, float... values) {
        TemporalBin bin = new TemporalBin(index, values.length);
        System.arraycopy(values, 0, bin.getFeatureValues(), 0, values.length);
        return bin;
    }

    private static int outputIndex(SEAGrid grid, long sourceIndex) {
        int sourceRow = grid.getRowIndex(sourceIndex);
        int outputRow = grid.getNumRows() - 1 - sourceRow;
        long column = sourceIndex - grid.getFirstBinIndex(sourceRow);
        return (int) (grid.getFirstBinIndex(outputRow) + column);
    }

    private static final class TrackingSource implements TemporalBinSource {

        private final List<TemporalBin>[] parts;
        private boolean opened;
        private boolean closed;
        private int processedPartCount;

        @SafeVarargs
        private TrackingSource(List<TemporalBin>... parts) {
            this.parts = parts;
        }

        @Override
        public int open() {
            opened = true;
            return parts.length;
        }

        @Override
        public Iterator<? extends TemporalBin> getPart(int index) {
            return parts[index].iterator();
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
