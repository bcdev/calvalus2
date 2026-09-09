package com.bc.calvalus.processing.mosaic2;

/*
 * Copyright (C) 2018 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import org.esa.snap.binning.AbstractAggregator;
import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.AggregatorDescriptor;
import org.esa.snap.binning.BinContext;
import org.esa.snap.binning.Observation;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.Vector;
import org.esa.snap.binning.WritableVector;
import org.esa.snap.core.gpf.annotations.Parameter;

import java.util.Arrays;

/**
 * An aggregator that computes the minimum and maximum values.
 */
public class AggregatorCube extends AbstractAggregator {

    private final int varIndex;

    public AggregatorCube(VariableContext varCtx, String varName, String targetVarName) {
        super(Descriptor.NAME, new String[] { varName }, new String[] { varName }, new String[] { targetVarName });

        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varName == null) {
            throw new NullPointerException("varName");
        }
        this.varIndex = varCtx.getVariableIndex(varName);
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {}

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {}

    @Override
    public void aggregateSpatial(BinContext ctx, Observation observationVector, WritableVector spatialVector) {}

    @Override
    public void completeSpatial(BinContext ctx, int numObs, WritableVector numSpatialObs) {}

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs,
                                  WritableVector temporalVector) {}

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {}

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {}

    @Override
    public String toString() {
        return "AggregatorCube";
    }

    public static class Config extends AggregatorConfig {
        /*
        {
             "type": "TOptCube",
             "varNames": "CHL,CHL_algo,TUR,TUR_algo,SDD,adg,TIME",
             "timeAxisLength": 30,
             "yAxisLength": 5110,
             "xAxisLength": 4321,
             "chunkSizeX": 128,
             "chunkSizeY": 128,
             "chunkSizeT": 30,
             "encoding": "littleendian",
             "compression": "zlib,1",
             "jsonFormattedCubeMetadataStr": "{\"project\":\"okosat\",\"creator\":\"Brockmann Consult GmbH\"}"
         }
         */

        @Parameter(label = "Type", notEmpty = true, notNull = true, description = "constant value TOptCube")
        String type;
        @Parameter(label = "Band names", notEmpty = true, notNull = true, description = "Source bands to be included in the cube")
        String varNames;
        @Parameter(label = "Cube length", description = "Length of the time axis of the cube")
        int timeAxisLength;
        @Parameter(label = "Cube height", description = "Length of the y axis of the cube")
        int yAxisLength;
        @Parameter(label = "Cube width", description = "Length of the x axis of the cube")
        int xAxisLength;
        @Parameter(label = "Cube time chunk size", description = "Length of one chunk of the time axis of the cube")
        int chunkSizeT;
        @Parameter(label = "Cube height chunk size", description = "Length of one chunk of the y axis of the cube")
        int chunkSizeY;
        @Parameter(label = "Cube width chunk size", description = "Length of one chunk of the x axis of the cube")
        int chunkSizeX;
        @Parameter(label = "Number encoding", description = "either littleendian or bigendian")
        String encoding;
        @Parameter(label = "Compression method and parameters", description = "e.g. zlib,1")
        String compression;
        @Parameter(label = "Metadata", description = "JSON dict with key-value pairs")
        String jsonFormattedCubeMetadataStr;


        public Config() {
            this(null, null,0,0,0,0,0,0, null, null, null);
        }

        public Config(String type, String varNames, int timeAxisLength, int yAxisLength, int xAxisLength, int chunkSizeT, int chunkSizeY, int chunkSizeX, String encoding, String compression, String jsonFormattedCubeMetadataStr) {
            super(Descriptor.NAME);
            this.type = type;
            this.varNames = varNames;
            this.timeAxisLength = timeAxisLength;
            this.yAxisLength = yAxisLength;
            this.xAxisLength = xAxisLength;
            this.chunkSizeT = chunkSizeT;
            this.chunkSizeY= chunkSizeY;
            this.chunkSizeX = chunkSizeX;
            this.encoding = encoding;
            this.compression = compression;
            this.jsonFormattedCubeMetadataStr = jsonFormattedCubeMetadataStr;
        }

    }

    public static class Descriptor implements AggregatorDescriptor {

        public static final String NAME = "TOptCube";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Aggregator createAggregator(VariableContext varCtx, AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            //return new AggregatorCube(varCtx, config.varName, targetName);
            return null;
        }

        @Override
        public AggregatorConfig createConfig() {
            return new Config();
        }

        @Override
        public String[] getSourceVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            return config.varNames.split(",");
        }

        @Override
        public String[] getTargetVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            return config.varNames.split(",");
        }
    }
}
