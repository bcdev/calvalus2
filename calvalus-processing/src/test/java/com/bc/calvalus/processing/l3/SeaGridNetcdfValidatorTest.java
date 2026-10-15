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
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SeaGridNetcdfValidatorTest {

    private File outputFile;

    @Before
    public void setUp() throws IOException {
        outputFile = File.createTempFile("calvalus-seagrid-validator-", ".nc");
    }

    @After
    public void tearDown() {
        if (outputFile != null && outputFile.exists() && !outputFile.delete()) {
            outputFile.deleteOnExit();
        }
    }

    @Test
    public void acceptsWriterOutput() throws Exception {
        SEAGrid grid = new SEAGrid(4);
        TemporalBin bin = new TemporalBin(2, 1);
        bin.getFeatureValues()[0] = 42.0f;

        SeaGridNetcdfFormatter.write(outputFile,
                                     grid,
                                     new SinglePartSource(bin),
                                     new String[]{"science_value"},
                                     ProductData.UTC.create(new Date(1659312000000L), 0),
                                     NetcdfFileWriter.Version.netcdf3);

        assertTrue(SeaGridNetcdfValidator.validate(outputFile, 4).isEmpty());
    }

    @Test
    public void reportsScienceVariableWithoutTimeDimension() throws Exception {
        writeInvalidStructure(outputFile, new SEAGrid(4));

        List<String> problems = SeaGridNetcdfValidator.validate(outputFile, 4);

        assertEquals(1, problems.size());
        assertEquals("Variable 'science_value' has dimensions [bin_index]; expected [time, bin_index].",
                     problems.get(0));
    }

    @Test
    public void acceptsConfiguredReferenceProduct() throws Exception {
        String referencePath = System.getProperty("calvalus.test.referenceNetcdf");
        Assume.assumeTrue("Set -Dcalvalus.test.referenceNetcdf=<file> to validate the reference product.",
                          referencePath != null && !referencePath.trim().isEmpty());

        File referenceFile = new File(referencePath);
        assertTrue("Reference product does not exist: " + referenceFile, referenceFile.isFile());
        List<String> problems = SeaGridNetcdfValidator.validate(referenceFile, 4320);
        assertTrue(problems.toString(), problems.isEmpty());
    }

    private static void writeInvalidStructure(File file, SEAGrid grid) throws Exception {
        NetcdfFileWriter writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3,
                                                             file.getAbsolutePath());
        Dimension time = writer.addDimension("time", 1);
        Dimension binIndex = writer.addDimension("bin_index", (int) grid.getNumBins());
        writer.addVariable("time", DataType.INT, Arrays.asList(time));
        Variable crs = writer.addVariable("crs", DataType.INT, Arrays.asList(time));
        crs.addAttribute(new Attribute("grid_mapping_name", "1D binned sinusoidal"));
        crs.addAttribute(new Attribute("number_of_latitude_rows", grid.getNumRows()));
        crs.addAttribute(new Attribute("total_number_of_bins", (int) grid.getNumBins()));
        writer.addVariable("lat", DataType.FLOAT, Arrays.asList(binIndex));
        writer.addVariable("lon", DataType.FLOAT, Arrays.asList(binIndex));
        Variable science = writer.addVariable("science_value", DataType.FLOAT, Arrays.asList(binIndex));
        science.addAttribute(new Attribute("_FillValue", Float.NaN));
        try {
            writer.create();
            writer.write(science, Array.factory(DataType.FLOAT,
                                                new int[]{(int) grid.getNumBins()},
                                                new float[(int) grid.getNumBins()]));
        } finally {
            writer.close();
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
