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
import org.esa.snap.core.datamodel.ProductData;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;

/**
 * Opt-in smoke test for the native NetCDF-4-classic writer.
 */
public class SeaGridNetcdf4SmokeTest {

    private File outputFile;

    @Before
    public void setUp() throws IOException {
        Assume.assumeTrue("Enable with -Dcalvalus.test.netcdf4=true.",
                          Boolean.getBoolean("calvalus.test.netcdf4"));
        outputFile = File.createTempFile("calvalus-seagrid-netcdf4-", ".nc");
    }

    @After
    public void tearDown() {
        if (outputFile != null && outputFile.exists() && !outputFile.delete()) {
            outputFile.deleteOnExit();
        }
    }

    @Test
    public void writesAndReadsNetcdf4Classic() throws Exception {
        SEAGrid grid = new SEAGrid(4);
        TemporalBin bin = new TemporalBin(2, 1);
        bin.getFeatureValues()[0] = 42.0f;

        SeaGridNetcdfFormatter.write(outputFile,
                                     grid,
                                     new SinglePartSource(bin),
                                     new String[]{"science_value"},
                                     ProductData.UTC.create(new Date(1659312000000L), 0));

        assertTrue(SeaGridNetcdfValidator.validate(outputFile, 4).isEmpty());
    }

    private static final class SinglePartSource implements TemporalBinSource {

        private final Iterable<TemporalBin> bins;

        private SinglePartSource(TemporalBin... bins) {
            this.bins = Collections.unmodifiableList(Arrays.asList(bins));
        }

        @Override
        public int open() {
            return 1;
        }

        @Override
        public Iterator<? extends TemporalBin> getPart(int index) {
            return bins.iterator();
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) {
        }

        @Override
        public void close() {
        }
    }
}

