/*
 * Copyright (C) 2026 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 */

package com.bc.calvalus.processing.l3;

import org.esa.snap.binning.support.SEAGrid;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates the structural parts of a flattened sinusoidal NetCDF product.
 * Product-specific names, types, and metadata remain outside this contract.
 */
public final class SeaGridNetcdfValidator {

    private static final Set<String> STRUCTURAL_VARIABLE_NAMES = new HashSet<String>(
            Arrays.asList("time", "bin_index", "lat", "lon", "crs"));

    private SeaGridNetcdfValidator() {
    }

    /**
     * Returns all structural problems found in {@code inputFile}.
     */
    public static List<String> validate(File inputFile, int numRows) throws IOException {
        if (inputFile == null) {
            throw new NullPointerException("Input file is required.");
        }
        if (!inputFile.isFile()) {
            throw new IOException("NetCDF file does not exist: " + inputFile.getAbsolutePath());
        }

        SEAGrid grid = new SEAGrid(numRows);
        NetcdfFile netcdfFile = NetcdfFile.open(inputFile.getAbsolutePath());
        try {
            return validate(netcdfFile, grid);
        } finally {
            netcdfFile.close();
        }
    }

    static List<String> validate(NetcdfFile netcdfFile, SEAGrid grid) {
        List<String> problems = new ArrayList<String>();
        long expectedBinCount = grid.getNumBins();

        expectDimension(netcdfFile, "time", 1, problems);
        expectDimension(netcdfFile, "bin_index", expectedBinCount, problems);

        expectVariableDimensions(netcdfFile, "time", problems, "time");
        expectVariableDimensions(netcdfFile, "crs", problems, "time");
        expectVariableDimensions(netcdfFile, "lat", problems, "bin_index");
        expectVariableDimensions(netcdfFile, "lon", problems, "bin_index");

        Variable crs = netcdfFile.findVariable("crs");
        if (crs != null) {
            expectStringAttribute(crs, "grid_mapping_name", "1D binned sinusoidal", problems);
            expectNumericAttribute(crs, "number_of_latitude_rows", grid.getNumRows(), problems);
            expectNumericAttribute(crs, "total_number_of_bins", expectedBinCount, problems);
        }

        int scienceVariableCount = 0;
        for (Variable variable : netcdfFile.getVariables()) {
            if (!STRUCTURAL_VARIABLE_NAMES.contains(variable.getShortName())) {
                scienceVariableCount++;
                expectDimensions(variable, problems, "time", "bin_index");
            }
        }
        if (scienceVariableCount == 0) {
            problems.add("No science variables were found.");
        }

        return Collections.unmodifiableList(problems);
    }

    /**
     * Command-line entry point for validating local or downloaded products.
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: SeaGridNetcdfValidator <netcdf-file> <number-of-latitude-rows>");
            System.exit(2);
        }

        final int numRows;
        try {
            numRows = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid number of latitude rows: " + args[1]);
            System.exit(2);
            return;
        }

        File inputFile = new File(args[0]);
        List<String> problems = validate(inputFile, numRows);
        if (problems.isEmpty()) {
            System.out.println("Valid flattened sinusoidal structure: " + inputFile.getAbsolutePath());
            return;
        }

        System.err.println("Invalid flattened sinusoidal structure: " + inputFile.getAbsolutePath());
        for (String problem : problems) {
            System.err.println("- " + problem);
        }
        System.exit(1);
    }

    private static void expectDimension(NetcdfFile netcdfFile,
                                        String name,
                                        long expectedLength,
                                        List<String> problems) {
        Dimension dimension = netcdfFile.findDimension(name);
        if (dimension == null) {
            problems.add("Missing dimension '" + name + "'.");
        } else if (dimension.getLength() != expectedLength) {
            problems.add("Dimension '" + name + "' has length " + dimension.getLength() +
                         "; expected " + expectedLength + '.');
        }
    }

    private static void expectVariableDimensions(NetcdfFile netcdfFile,
                                                 String variableName,
                                                 List<String> problems,
                                                 String... expectedDimensions) {
        Variable variable = netcdfFile.findVariable(variableName);
        if (variable == null) {
            problems.add("Missing variable '" + variableName + "'.");
        } else {
            expectDimensions(variable, problems, expectedDimensions);
        }
    }

    private static void expectDimensions(Variable variable,
                                         List<String> problems,
                                         String... expectedDimensions) {
        List<Dimension> dimensions = variable.getDimensions();
        if (dimensions.size() != expectedDimensions.length) {
            problems.add("Variable '" + variable.getShortName() + "' has dimensions " +
                         dimensionNames(dimensions) + "; expected " + Arrays.toString(expectedDimensions) + '.');
            return;
        }
        for (int index = 0; index < expectedDimensions.length; index++) {
            if (!expectedDimensions[index].equals(dimensions.get(index).getShortName())) {
                problems.add("Variable '" + variable.getShortName() + "' has dimensions " +
                             dimensionNames(dimensions) + "; expected " + Arrays.toString(expectedDimensions) + '.');
                return;
            }
        }
    }

    private static String dimensionNames(List<Dimension> dimensions) {
        List<String> names = new ArrayList<String>(dimensions.size());
        for (Dimension dimension : dimensions) {
            names.add(dimension.getShortName());
        }
        return names.toString();
    }

    private static void expectStringAttribute(Variable variable,
                                              String attributeName,
                                              String expectedValue,
                                              List<String> problems) {
        Attribute attribute = variable.findAttribute(attributeName);
        if (attribute == null) {
            problems.add("Variable '" + variable.getShortName() + "' is missing attribute '" +
                         attributeName + "'.");
        } else if (!expectedValue.equals(attribute.getStringValue())) {
            problems.add("Variable '" + variable.getShortName() + "' attribute '" + attributeName +
                         "' is '" + attribute.getStringValue() + "'; expected '" + expectedValue + "'.");
        }
    }

    private static void expectNumericAttribute(Variable variable,
                                               String attributeName,
                                               long expectedValue,
                                               List<String> problems) {
        Attribute attribute = variable.findAttribute(attributeName);
        if (attribute == null || attribute.getNumericValue() == null) {
            problems.add("Variable '" + variable.getShortName() + "' is missing numeric attribute '" +
                         attributeName + "'.");
        } else if (attribute.getNumericValue().longValue() != expectedValue) {
            problems.add("Variable '" + variable.getShortName() + "' attribute '" + attributeName +
                         "' is " + attribute.getNumericValue() + "; expected " + expectedValue + '.');
        }
    }
}

