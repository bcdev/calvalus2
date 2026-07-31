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
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.dataio.netcdf.AbstractNetCdfWriterPlugIn;
import org.esa.snap.dataio.netcdf.DefaultNetCdfWriter;
import org.esa.snap.dataio.netcdf.ProfileWriteContext;
import org.esa.snap.dataio.netcdf.ProfileWriteContextImpl;
import org.esa.snap.dataio.netcdf.metadata.profiles.beam.BeamMetadataPart;
import org.esa.snap.dataio.netcdf.metadata.profiles.beam.BeamNetCdf4WriterPlugIn;
import org.esa.snap.dataio.netcdf.metadata.profiles.beam.BeamNetCdfWriterPlugIn;
import org.esa.snap.dataio.netcdf.metadata.profiles.cf.CfTimePart;
import org.esa.snap.dataio.netcdf.nc.N3Variable;
import org.esa.snap.dataio.netcdf.nc.N4Variable;
import org.esa.snap.dataio.netcdf.nc.NFileWriteable;
import org.esa.snap.dataio.netcdf.nc.NVariable;
import org.esa.snap.dataio.netcdf.util.DataTypeUtils;
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
 * {@code (bin_index)}. The bin-row order is south to north, matching the
 * reference; longitude order within each row is preserved.
 * The Calvalus processing graph and core BEAM product attributes are retained;
 * remaining product-specific metadata is left to a later post-processing step.
 */
final class SeaGridNetcdfFormatter {

    private static final int BUFFER_SIZE = 8192;
    private static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;
    private static final String PRODUCT_TYPE = "BINNED-L3";
    private static final Set<String> RESERVED_VARIABLE_NAMES = new HashSet<String>(
            Arrays.asList("metadata", "time", "bin_index", "lat", "lon", "crs",
                          "num_obs", "num_passes"));

    private SeaGridNetcdfFormatter() {
    }

    static void write(File outputFile,
                      SEAGrid planetaryGrid,
                      TemporalBinSource temporalBinSource,
                      String[] featureNames,
                      ProductData.UTC startTime) throws IOException {
        write(outputFile, planetaryGrid, temporalBinSource, featureNames, startTime, startTime,
              new MetadataElement[0],
              NetcdfFileWriter.Version.netcdf4_classic);
    }

    static void write(File outputFile,
                      SEAGrid planetaryGrid,
                      TemporalBinSource temporalBinSource,
                      String[] featureNames,
                      ProductData.UTC startTime,
                      ProductData.UTC endTime,
                      MetadataElement... metadataElements) throws IOException {
        write(outputFile, planetaryGrid, temporalBinSource, featureNames, startTime, endTime,
              metadataElements, NetcdfFileWriter.Version.netcdf4_classic);
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
        write(outputFile, planetaryGrid, temporalBinSource, featureNames, startTime, startTime,
              new MetadataElement[0], version);
    }

    static void write(File outputFile,
                      SEAGrid planetaryGrid,
                      TemporalBinSource temporalBinSource,
                      String[] featureNames,
                      ProductData.UTC startTime,
                      ProductData.UTC endTime,
                      MetadataElement[] metadataElements,
                      NetcdfFileWriter.Version version) throws IOException {
        validateArguments(outputFile, planetaryGrid, temporalBinSource, featureNames);
        validateMirroredRows(planetaryGrid);

        final long numBinsLong = planetaryGrid.getNumBins();
        if (numBinsLong > Integer.MAX_VALUE) {
            throw new IOException("The sinusoidal bin count exceeds the NetCDF dimension limit: " + numBinsLong);
        }
        final int numBins = (int) numBinsLong;

        NetcdfFileWriter writer = NetcdfFileWriter.createNew(version, outputFile.getAbsolutePath());
        writer.setFill(true);
        writer.setLargeFile(true);

        writer.addGlobalAttribute("Conventions", "CF-1.7");
        writer.addGlobalAttribute("product_type", PRODUCT_TYPE);
        addMetadataAndTimeAttributes(writer, startTime, endTime, metadataElements, version);

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

        final List<Dimension> dataDimensions = Arrays.asList(timeDimension, binIndexDimension);
        final Variable numObsVariable = writer.addVariable("num_obs", DataType.INT, dataDimensions);
        numObsVariable.addAttribute(new Attribute("_FillValue", -1));
        numObsVariable.addAttribute(new Attribute("coordinates", "lat lon"));
        numObsVariable.addAttribute(new Attribute("grid_mapping", "crs"));

        final Variable numPassesVariable = writer.addVariable("num_passes", DataType.SHORT, dataDimensions);
        numPassesVariable.addAttribute(new Attribute("_FillValue", (short) -1));
        numPassesVariable.addAttribute(new Attribute("coordinates", "lat lon"));
        numPassesVariable.addAttribute(new Attribute("grid_mapping", "crs"));

        final List<Variable> featureVariables = new ArrayList<Variable>(featureNames.length);
        for (String featureName : featureNames) {
            Variable featureVariable = writer.addVariable(featureName, DataType.FLOAT, dataDimensions);
            featureVariable.addAttribute(new Attribute("_FillValue", Float.NaN));
            featureVariable.addAttribute(new Attribute("coordinates", "lat lon"));
            featureVariable.addAttribute(new Attribute("grid_mapping", "crs"));
            featureVariables.add(featureVariable);
        }

        boolean sourceOpened = false;
        try {
            writer.create();
            writeScalarVariables(writer, timeVariable, crsVariable, startTime);
            writeCoordinates(writer, planetaryGrid, latitudeVariable, longitudeVariable);

            final FeatureBuffer featureBuffer =
                    new FeatureBuffer(writer, numObsVariable, numPassesVariable,
                                      featureVariables, planetaryGrid);
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

    private static void addMetadataAndTimeAttributes(NetcdfFileWriter writer,
                                                     ProductData.UTC startTime,
                                                     ProductData.UTC endTime,
                                                     MetadataElement[] metadataElements,
                                                     NetcdfFileWriter.Version version) throws IOException {
        final boolean netcdf4 = version == NetcdfFileWriter.Version.netcdf4 ||
                                version == NetcdfFileWriter.Version.netcdf4_classic;
        final ExistingFileWriteable writeable = new ExistingFileWriteable(writer, netcdf4);
        final ProfileWriteContext context = new ProfileWriteContextImpl(writeable);

        final Product metadataProduct = new Product("metadata", PRODUCT_TYPE, 1, 1);
        final AbstractNetCdfWriterPlugIn writerPlugIn =
                netcdf4 ? new BeamNetCdf4WriterPlugIn() : new BeamNetCdfWriterPlugIn();
        metadataProduct.setProductWriter(new DefaultNetCdfWriter(writerPlugIn));
        metadataProduct.setStartTime(startTime);
        metadataProduct.setEndTime(endTime);
        if (metadataElements != null) {
            for (MetadataElement metadataElement : metadataElements) {
                if (metadataElement != null) {
                    metadataProduct.getMetadataRoot().addElement(metadataElement);
                }
            }
        }

        new BeamMetadataPart().preEncode(context, metadataProduct);
        new CfTimePart().preEncode(context, metadataProduct);
    }

    private static void validateArguments(File outputFile,
                                          SEAGrid planetaryGrid,
                                          TemporalBinSource temporalBinSource,
                                          String[] featureNames) {
        if (outputFile == null || planetaryGrid == null || temporalBinSource == null ||
            featureNames == null) {
            throw new NullPointerException(
                    "Output file, grid, bin source, and feature names are required.");
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

    private static void validateMirroredRows(SEAGrid planetaryGrid) throws IOException {
        int numRows = planetaryGrid.getNumRows();
        for (int row = 0; row < numRows / 2; row++) {
            int mirroredRow = numRows - 1 - row;
            if (planetaryGrid.getNumCols(row) != planetaryGrid.getNumCols(mirroredRow)) {
                throw new IOException("Cannot reverse SEAGrid rows " + row + " and " + mirroredRow +
                                      " because their column counts differ.");
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
                                         Variable longitudeVariable)
            throws IOException, InvalidRangeException {
        int numRows = planetaryGrid.getNumRows();
        for (int outputRow = 0; outputRow < numRows; outputRow++) {
            // SNAP's SEAGrid enumerates rows north-to-south. Mirror them on output to work around
            // this limitation and produce the south-to-north bin order used by standard L3 products.
            int sourceRow = numRows - 1 - outputRow;
            int numCols = planetaryGrid.getNumCols(outputRow);
            long outputRowStart = planetaryGrid.getFirstBinIndex(outputRow);
            long sourceRowStart = planetaryGrid.getFirstBinIndex(sourceRow);
            for (int columnOrigin = 0; columnOrigin < numCols; columnOrigin += BUFFER_SIZE) {
                int length = Math.min(BUFFER_SIZE, numCols - columnOrigin);
                float[] latitudes = new float[length];
                float[] longitudes = new float[length];
                for (int offset = 0; offset < length; offset++) {
                    double[] center = planetaryGrid.getCenterLatLon(
                            sourceRowStart + columnOrigin + offset);
                    latitudes[offset] = (float) center[0];
                    longitudes[offset] = (float) center[1];
                }
                int origin = (int) (outputRowStart + columnOrigin);
                writer.write(latitudeVariable, new int[]{origin},
                             Array.factory(DataType.FLOAT, new int[]{length}, latitudes));
                writer.write(longitudeVariable, new int[]{origin},
                             Array.factory(DataType.FLOAT, new int[]{length}, longitudes));
            }
        }
    }

    private static final class FeatureBuffer {

        private final NetcdfFileWriter writer;
        private final Variable numObsVariable;
        private final Variable numPassesVariable;
        private final List<Variable> variables;
        private final SEAGrid planetaryGrid;
        private final long numBins;
        private final int[] numObsValues;
        private final short[] numPassesValues;
        private final float[][] values;

        private long lastIndex = -1;
        private int sourceRow = -1;
        private int minColumn;
        private int maxColumn;

        private FeatureBuffer(NetcdfFileWriter writer,
                              Variable numObsVariable,
                              Variable numPassesVariable,
                              List<Variable> variables,
                              SEAGrid planetaryGrid) {
            this.writer = writer;
            this.numObsVariable = numObsVariable;
            this.numPassesVariable = numPassesVariable;
            this.variables = variables;
            this.planetaryGrid = planetaryGrid;
            this.numBins = planetaryGrid.getNumBins();
            int maxNumCols = 0;
            for (int row = 0; row < planetaryGrid.getNumRows(); row++) {
                maxNumCols = Math.max(maxNumCols, planetaryGrid.getNumCols(row));
            }
            numObsValues = new int[maxNumCols];
            numPassesValues = new short[maxNumCols];
            values = new float[variables.size()][maxNumCols];
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
            int numPasses = temporalBin.getNumPasses();
            if (numPasses < 0 || numPasses > Short.MAX_VALUE) {
                throw new IOException("Temporal bin " + binIndex + " has num_passes " + numPasses +
                                      "; expected a value between 0 and " + Short.MAX_VALUE + '.');
            }
            int binRow = planetaryGrid.getRowIndex(binIndex);
            if (sourceRow != binRow) {
                flush();
                reset(binRow);
            }

            int column = (int) (binIndex - planetaryGrid.getFirstBinIndex(sourceRow));
            numObsValues[column] = temporalBin.getNumObs();
            numPassesValues[column] = (short) numPasses;
            float[] featureValues = temporalBin.getFeatureValues();
            for (int featureIndex = 0; featureIndex < values.length; featureIndex++) {
                values[featureIndex][column] = featureValues[featureIndex];
            }
            minColumn = Math.min(minColumn, column);
            maxColumn = Math.max(maxColumn, column);
            lastIndex = binIndex;
        }

        private void reset(int newSourceRow) {
            sourceRow = newSourceRow;
            int numCols = planetaryGrid.getNumCols(sourceRow);
            Arrays.fill(numObsValues, 0, numCols, -1);
            Arrays.fill(numPassesValues, 0, numCols, (short) -1);
            for (float[] featureValues : values) {
                Arrays.fill(featureValues, 0, numCols, Float.NaN);
            }
            minColumn = numCols;
            maxColumn = -1;
        }

        private void flush() throws IOException, InvalidRangeException {
            if (sourceRow < 0 || maxColumn < minColumn) {
                return;
            }
            // Apply the same SNAP-to-standard row-order conversion as for the coordinate variables.
            int outputRow = planetaryGrid.getNumRows() - 1 - sourceRow;
            long outputRowStart = planetaryGrid.getFirstBinIndex(outputRow);
            int length = maxColumn - minColumn + 1;
            int[] origin = new int[]{0, (int) (outputRowStart + minColumn)};
            int[] numObsData = Arrays.copyOfRange(numObsValues, minColumn, maxColumn + 1);
            writer.write(numObsVariable, origin,
                         Array.factory(DataType.INT, new int[]{1, length}, numObsData));
            short[] numPassesData = Arrays.copyOfRange(numPassesValues, minColumn, maxColumn + 1);
            writer.write(numPassesVariable, origin,
                         Array.factory(DataType.SHORT, new int[]{1, length}, numPassesData));
            for (int featureIndex = 0; featureIndex < variables.size(); featureIndex++) {
                float[] data = Arrays.copyOfRange(values[featureIndex], minColumn, maxColumn + 1);
                writer.write(variables.get(featureIndex), origin,
                             Array.factory(DataType.FLOAT, new int[]{1, length}, data));
            }
            sourceRow = -1;
        }
    }

    /**
     * Adapts SNAP's metadata profile writer to the already-open definition
     * phase of this formatter's NetCDF writer.
     */
    private static final class ExistingFileWriteable extends NFileWriteable {

        private final boolean netcdf4;

        private ExistingFileWriteable(NetcdfFileWriter writer, boolean netcdf4) {
            this.netcdfFileWriter = writer;
            this.netcdf4 = netcdf4;
        }

        @Override
        public NVariable addScalarVariable(String name, DataType dataType) {
            Variable variable = netcdfFileWriter.addVariable(
                    null, name, dataType, new ArrayList<Dimension>());
            NVariable nVariable = netcdf4
                    ? new N4Variable(variable, null, netcdfFileWriter)
                    : new N3Variable(variable, netcdfFileWriter);
            variables.put(name, nVariable);
            return nVariable;
        }

        @Override
        public NVariable addVariable(String name,
                                     DataType dataType,
                                     boolean unsigned,
                                     java.awt.Dimension tileSize,
                                     String dimensions,
                                     int compressionLevel) {
            throw new UnsupportedOperationException(
                    "The metadata adapter only supports scalar variables.");
        }

        @Override
        public DataType getNetcdfDataType(int dataType) {
            return netcdf4
                    ? DataTypeUtils.getNetcdf4DataType(dataType)
                    : DataTypeUtils.getNetcdfDataType(dataType);
        }
    }
}
