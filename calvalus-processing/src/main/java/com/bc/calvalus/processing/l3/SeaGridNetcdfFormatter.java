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
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Writes a fully populated, flattened sinusoidal NetCDF file.
 *
 * The structural model follows the OC-CCI reference products: science
 * variables use {@code (time, bin_index)}, while latitude and longitude use
 * {@code (bin_index)}. Product-specific metadata is intentionally left to a
 * later post-processing step.
 */
final class SeaGridNetcdfFormatter {

    private static final int BUFFER_SIZE = 8192;
    private static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;
    private static final Set<String> RESERVED_VARIABLE_NAMES = new HashSet<String>(
            Arrays.asList("time", "bin_index", "lat", "lon", "crs"));

    private SeaGridNetcdfFormatter() {
    }

    static void write(File outputFile,
                      SEAGrid planetaryGrid,
                      TemporalBinSource temporalBinSource,
                      String[] featureNames,
                      ProductData.UTC startTime) throws IOException {
        write(outputFile, planetaryGrid, temporalBinSource, featureNames, startTime,
              NetcdfFileWriter.Version.netcdf4_classic);
    }

    /**
     * Package-private format selection keeps the structural test independent
     * of the native NetCDF-4 library. Production always uses NetCDF-4 classic.
     */
    static void write(File outputFile,
                      SEAGrid planetaryGrid,
                      TemporalBinSource temporalBinSource,
                      String[] featureNames,
                      ProductData.UTC startTime,
                      NetcdfFileWriter.Version version) throws IOException {
        validateArguments(outputFile, planetaryGrid, temporalBinSource, featureNames);

        final long numBinsLong = planetaryGrid.getNumBins();
        if (numBinsLong > Integer.MAX_VALUE) {
            throw new IOException("The sinusoidal bin count exceeds the NetCDF dimension limit: " + numBinsLong);
        }
        final int numBins = (int) numBinsLong;

        NetcdfFileWriter writer = NetcdfFileWriter.createNew(version, outputFile.getAbsolutePath());
        writer.setFill(true);
        writer.setLargeFile(true);

        final Dimension timeDimension = writer.addDimension("time", 1);
        final Dimension binIndexDimension = writer.addDimension("bin_index", numBins);

        final Variable timeVariable = writer.addVariable("time", DataType.INT,
                                                         Arrays.asList(timeDimension));
        timeVariable.addAttribute(new Attribute("axis", "T"));
        timeVariable.addAttribute(new Attribute("standard_name", "time"));
        timeVariable.addAttribute(new Attribute("units", "days since 1970-01-01"));

        final Variable crsVariable = writer.addVariable("crs", DataType.INT,
                                                        Arrays.asList(timeDimension));
        crsVariable.addAttribute(new Attribute("grid_mapping_name", "1D binned sinusoidal"));
        crsVariable.addAttribute(new Attribute("number_of_latitude_rows", planetaryGrid.getNumRows()));
        crsVariable.addAttribute(new Attribute("total_number_of_bins", numBins));

        final Variable latitudeVariable = writer.addVariable("lat", DataType.FLOAT,
                                                             Arrays.asList(binIndexDimension));
        latitudeVariable.addAttribute(new Attribute("standard_name", "latitude"));
        latitudeVariable.addAttribute(new Attribute("units", "degrees_north"));
        latitudeVariable.addAttribute(new Attribute("axis", "Y"));

        final Variable longitudeVariable = writer.addVariable("lon", DataType.FLOAT,
                                                              Arrays.asList(binIndexDimension));
        longitudeVariable.addAttribute(new Attribute("standard_name", "longitude"));
        longitudeVariable.addAttribute(new Attribute("units", "degrees_east"));
        longitudeVariable.addAttribute(new Attribute("axis", "X"));

        final List<Variable> featureVariables = new ArrayList<Variable>(featureNames.length);
        for (String featureName : featureNames) {
            Variable featureVariable = writer.addVariable(featureName, DataType.FLOAT,
                                                           Arrays.asList(timeDimension, binIndexDimension));
            featureVariable.addAttribute(new Attribute("_FillValue", Float.NaN));
            featureVariable.addAttribute(new Attribute("coordinates", "lat lon"));
            featureVariable.addAttribute(new Attribute("grid_mapping", "crs"));
            featureVariables.add(featureVariable);
        }

        writer.addGlobalAttribute("Conventions", "CF-1.7");

        boolean sourceOpened = false;
        try {
            writer.create();
            writeScalarVariables(writer, timeVariable, crsVariable, startTime);
            writeCoordinates(writer, planetaryGrid, latitudeVariable, longitudeVariable, numBins);

            final FeatureBuffer featureBuffer = new FeatureBuffer(writer, featureVariables, numBins);
            final int partCount = temporalBinSource.open();
            sourceOpened = true;
            for (int partIndex = 0; partIndex < partCount; partIndex++) {
                Iterator<? extends TemporalBin> part = temporalBinSource.getPart(partIndex);
                while (part.hasNext()) {
                    featureBuffer.add(part.next());
                }
                temporalBinSource.partProcessed(partIndex, part);
            }
            featureBuffer.flush();
        } catch (InvalidRangeException e) {
            throw new IOException("Failed to write sinusoidal NetCDF data.", e);
        } finally {
            IOException closeFailure = null;
            if (sourceOpened) {
                try {
                    temporalBinSource.close();
                } catch (IOException e) {
                    closeFailure = e;
                }
            }
            try {
                writer.close();
            } catch (IOException e) {
                if (closeFailure == null) {
                    closeFailure = e;
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static void validateArguments(File outputFile,
                                          SEAGrid planetaryGrid,
                                          TemporalBinSource temporalBinSource,
                                          String[] featureNames) {
        if (outputFile == null || planetaryGrid == null || temporalBinSource == null || featureNames == null) {
            throw new NullPointerException("Output file, grid, bin source, and feature names are required.");
        }
        if (featureNames.length == 0) {
            throw new IllegalArgumentException("At least one science variable is required.");
        }
        Set<String> uniqueNames = new HashSet<String>();
        for (String featureName : featureNames) {
            if (featureName == null || featureName.trim().isEmpty()) {
                throw new IllegalArgumentException("Science-variable names must not be empty.");
            }
            if (RESERVED_VARIABLE_NAMES.contains(featureName)) {
                throw new IllegalArgumentException("Reserved NetCDF variable name: " + featureName);
            }
            if (!uniqueNames.add(featureName)) {
                throw new IllegalArgumentException("Duplicate science-variable name: " + featureName);
            }
        }
    }

    private static void writeScalarVariables(NetcdfFileWriter writer,
                                             Variable timeVariable,
                                             Variable crsVariable,
                                             ProductData.UTC startTime)
            throws IOException, InvalidRangeException {
        int epochDay = startTime != null ? (int) (startTime.getAsDate().getTime() / MILLIS_PER_DAY) : 0;
        writer.write(timeVariable, Array.factory(DataType.INT, new int[]{1}, new int[]{epochDay}));
        writer.write(crsVariable, Array.factory(DataType.INT, new int[]{1}, new int[]{0}));
    }

    private static void writeCoordinates(NetcdfFileWriter writer,
                                         SEAGrid planetaryGrid,
                                         Variable latitudeVariable,
                                         Variable longitudeVariable,
                                         int numBins)
            throws IOException, InvalidRangeException {
        for (int origin = 0; origin < numBins; origin += BUFFER_SIZE) {
            int length = Math.min(BUFFER_SIZE, numBins - origin);
            float[] latitudes = new float[length];
            float[] longitudes = new float[length];
            for (int offset = 0; offset < length; offset++) {
                double[] center = planetaryGrid.getCenterLatLon((long) origin + offset);
                latitudes[offset] = (float) center[0];
                longitudes[offset] = (float) center[1];
            }
            writer.write(latitudeVariable, new int[]{origin},
                         Array.factory(DataType.FLOAT, new int[]{length}, latitudes));
            writer.write(longitudeVariable, new int[]{origin},
                         Array.factory(DataType.FLOAT, new int[]{length}, longitudes));
        }
    }

    private static final class FeatureBuffer {

        private final NetcdfFileWriter writer;
        private final List<Variable> variables;
        private final int numBins;
        private final float[][] values;

        private long startIndex = -1;
        private long lastIndex = -1;
        private int length;

        private FeatureBuffer(NetcdfFileWriter writer, List<Variable> variables, int numBins) {
            this.writer = writer;
            this.variables = variables;
            this.numBins = numBins;
            values = new float[variables.size()][BUFFER_SIZE];
        }

        private void add(TemporalBin temporalBin) throws IOException, InvalidRangeException {
            long binIndex = temporalBin.getIndex();
            if (binIndex < 0 || binIndex >= numBins) {
                throw new IOException("Temporal bin index outside the sinusoidal grid: " + binIndex);
            }
            if (binIndex <= lastIndex) {
                throw new IOException("Temporal bins must be ordered by increasing global bin index: " + binIndex);
            }
            if (temporalBin.getFeatureValues().length != variables.size()) {
                throw new IOException("Temporal bin " + binIndex + " has " +
                                      temporalBin.getFeatureValues().length + " features; expected " +
                                      variables.size() + '.');
            }
            if (startIndex < 0 || binIndex >= startIndex + BUFFER_SIZE) {
                flush();
                reset(binIndex);
            }

            int offset = (int) (binIndex - startIndex);
            for (int featureIndex = 0; featureIndex < values.length; featureIndex++) {
                values[featureIndex][offset] = temporalBin.getFeatureValues()[featureIndex];
            }
            length = Math.max(length, offset + 1);
            lastIndex = binIndex;
        }

        private void reset(long newStartIndex) {
            startIndex = newStartIndex;
            length = 0;
            for (float[] featureValues : values) {
                Arrays.fill(featureValues, Float.NaN);
            }
        }

        private void flush() throws IOException, InvalidRangeException {
            if (length == 0) {
                return;
            }
            int[] origin = new int[]{0, (int) startIndex};
            for (int featureIndex = 0; featureIndex < variables.size(); featureIndex++) {
                float[] data = Arrays.copyOf(values[featureIndex], length);
                writer.write(variables.get(featureIndex), origin,
                             Array.factory(DataType.FLOAT, new int[]{1, length}, data));
            }
            startIndex = -1;
            length = 0;
        }
    }
}
