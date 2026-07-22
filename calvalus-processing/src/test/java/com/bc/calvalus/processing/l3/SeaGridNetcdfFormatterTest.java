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
import org.junit.Before;
import org.junit.Test;
import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SeaGridNetcdfFormatterTest {

    private File outputFile;

    @Before
    public void setUp() throws IOException {
        outputFile = File.createTempFile("calvalus-isin-structure-", ".nc");
    }

    @After
    public void tearDown() {
        if (outputFile != null && outputFile.exists() && !outputFile.delete()) {
            outputFile.deleteOnExit();
        }
    }

    @Test
    public void writesReferenceDimensionModelFromSyntheticBins() throws Exception {
        SEAGrid grid = new SEAGrid(4);
        TemporalBin firstBin = createBin(0, 1.25f, 3.5f);
        TemporalBin secondBin = createBin(10, 2.5f, 7.0f);
        TemporalBinSource source = new SinglePartSource(firstBin, secondBin);

        SeaGridNetcdfFormatter.write(outputFile,
                                  grid,
                                  source,
                                  new String[]{"chlor_a", "total_nobs"},
                                  ProductData.UTC.create(new Date(1659312000000L), 0),
                                  NetcdfFileWriter.Version.netcdf3);

        NetcdfFile netcdfFile = NetcdfFile.open(outputFile.getAbsolutePath());
        try {
            Dimension time = netcdfFile.findDimension("time");
            Dimension binIndex = netcdfFile.findDimension("bin_index");
            assertNotNull(time);
            assertNotNull(binIndex);
            assertEquals(1, time.getLength());
            assertEquals(grid.getNumBins(), binIndex.getLength());

            assertDimensions(netcdfFile.findVariable("chlor_a"), "time", "bin_index");
            assertDimensions(netcdfFile.findVariable("total_nobs"), "time", "bin_index");
            assertDimensions(netcdfFile.findVariable("lat"), "bin_index");
            assertDimensions(netcdfFile.findVariable("lon"), "bin_index");
            assertDimensions(netcdfFile.findVariable("time"), "time");
            assertDimensions(netcdfFile.findVariable("crs"), "time");

            Variable crs = netcdfFile.findVariable("crs");
            assertEquals("1D binned sinusoidal",
                         crs.findAttribute("grid_mapping_name").getStringValue());
            assertEquals(4, crs.findAttribute("number_of_latitude_rows").getNumericValue().intValue());
            assertEquals(grid.getNumBins(),
                         crs.findAttribute("total_number_of_bins").getNumericValue().longValue());

            Array chlorA = netcdfFile.findVariable("chlor_a").read();
            Array totalNobs = netcdfFile.findVariable("total_nobs").read();
            assertEquals(1.25f, chlorA.getFloat(0), 0.0f);
            assertEquals(3.5f, totalNobs.getFloat(0), 0.0f);
            assertTrue(Float.isNaN(chlorA.getFloat(1)));
            assertEquals(2.5f, chlorA.getFloat(10), 0.0f);
            assertEquals(7.0f, totalNobs.getFloat(10), 0.0f);

            Attribute conventions = netcdfFile.findGlobalAttribute("Conventions");
            assertNotNull(conventions);
            assertEquals("CF-1.7", conventions.getStringValue());
        } finally {
            netcdfFile.close();
        }
    }

    @Test
    public void requestedResolutionsHaveExpectedGlobalBinCounts() {
        assertEquals(16501208L, new SEAGrid(3600).getNumBins());
        assertEquals(2640174L, new SEAGrid(1440).getNumBins());
    }

    private static TemporalBin createBin(long index, float... values) {
        TemporalBin bin = new TemporalBin(index, values.length);
        bin.setNumObs(1);
        bin.setNumPasses(1);
        System.arraycopy(values, 0, bin.getFeatureValues(), 0, values.length);
        return bin;
    }

    private static void assertDimensions(Variable variable, String... names) {
        assertNotNull(variable);
        assertEquals(names.length, variable.getDimensions().size());
        for (int index = 0; index < names.length; index++) {
            assertEquals(names[index], variable.getDimension(index).getShortName());
        }
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
            if (index != 0) {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
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
