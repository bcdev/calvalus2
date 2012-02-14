/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package org.esa.beam.binning;

import com.bc.calvalus.binning.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.ProductData;

import java.awt.*;
import java.io.File;
import java.util.Iterator;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 *
 * @author Norman Fomferra
 */
public class Outputter {

    public static void output(BinningContext binningContext,
                              OutputterConfig outputterConfig,
                              Geometry roiGeometry,
                              ProductData.UTC startTime,
                              ProductData.UTC stopTime,
                              MetadataElement metadataElement,
                              TemporalBinSource temporalBinSource) throws Exception {

        if (binningContext.getBinManager().getAggregatorCount() == 0) {
            throw new IllegalArgumentException("Illegal binning context: aggregatorCount == 0");
        }

        final File outputFile = new File(outputterConfig.getOutputFile());
        final String outputType = outputterConfig.getOutputType();
        final String outputFormat = getOutputFormat(outputterConfig, outputFile);

        final BinningGrid binningGrid = binningContext.getBinningGrid();
        final Rectangle outputRegion = getOutputRegion(binningGrid, roiGeometry);

        final BinRasterizer binRasterizer;
        if (outputType.equalsIgnoreCase("Product")) {
            binRasterizer = new ProductBinRasterizer(binningContext,
                                                     outputFile,
                                                     outputFormat,
                                                     outputRegion,
                                                     getOutputPixelSize(binningGrid),
                                                     startTime,
                                                     stopTime,
                                                     metadataElement);
        } else {
            binRasterizer = new ImageBinRasterizer(binningContext,
                                                   outputFile,
                                                   outputFormat,
                                                   outputRegion,
                                                   outputterConfig.getBands(),
                                                   outputType.equalsIgnoreCase("RGB"));
        }

        output(binningContext, outputRegion, binRasterizer, temporalBinSource);
    }

    private static void output(BinningContext binningContext, Rectangle outputRegion, BinRasterizer binRasterizer, TemporalBinSource temporalBinSource) throws Exception {
        BinReprojector reprojector = new BinReprojector(binningContext, binRasterizer, outputRegion);
        final int partCount = temporalBinSource.open();
        reprojector.begin();
        for (int i = 0; i < partCount; i++) {
            final Iterator<? extends TemporalBin> part = temporalBinSource.getPart(i);
            reprojector.processPart(part);
            temporalBinSource.partProcessed(i, part);
        }
        reprojector.end();
        temporalBinSource.close();
    }

    private static Rectangle getOutputRegion(BinningGrid binningGrid, Geometry roiGeometry) {
        final double pixelSize = getOutputPixelSize(binningGrid);
        final int gridHeight = binningGrid.getNumRows();
        final int gridWidth = 2 * gridHeight;
        Rectangle outputRegion = new Rectangle(gridWidth, gridHeight);
        if (roiGeometry != null) {
            final Coordinate[] coordinates = roiGeometry.getBoundary().getCoordinates();
            double gxmin = Double.POSITIVE_INFINITY;
            double gxmax = Double.NEGATIVE_INFINITY;
            double gymin = Double.POSITIVE_INFINITY;
            double gymax = Double.NEGATIVE_INFINITY;
            for (Coordinate coordinate : coordinates) {
                gxmin = Math.min(gxmin, coordinate.x);
                gxmax = Math.max(gxmax, coordinate.x);
                gymin = Math.min(gymin, coordinate.y);
                gymax = Math.max(gymax, coordinate.y);
            }
            final int x = (int) Math.floor((180.0 + gxmin) / pixelSize);
            final int y = (int) Math.floor((90.0 - gymax) / pixelSize);
            final int width = (int) Math.ceil((gxmax - gxmin) / pixelSize);
            final int height = (int) Math.ceil((gymax - gymin) / pixelSize);
            final Rectangle unclippedOutputRegion = new Rectangle(x, y, width, height);
            outputRegion = unclippedOutputRegion.intersection(outputRegion);
        }
        return outputRegion;
    }

    private static double getOutputPixelSize(BinningGrid binningGrid) {
        return 180.0 / binningGrid.getNumRows();
    }

    private static String getOutputFormat(OutputterConfig outputterConfig, File outputFile) {
        final String fileName = outputFile.getName();
        final int extPos = fileName.lastIndexOf(".");
        String outputFileNameExt = fileName.substring(extPos + 1);
        String outputFormat = outputterConfig.getOutputFormat();
        if (outputFormat == null) {
            outputFormat = outputFileNameExt.equalsIgnoreCase("nc") ? "NetCDF"
                    : outputFileNameExt.equalsIgnoreCase("dim") ? "BEAM-DIMAP"
                    : outputFileNameExt.equalsIgnoreCase("tiff") ? "GeoTIFF"
                    : outputFileNameExt.equalsIgnoreCase("png") ? "PNG"
                    : outputFileNameExt.equalsIgnoreCase("jpg") ? "JPEG" : null;
        }
        if (outputFormat == null) {
            throw new IllegalArgumentException("No output format given");
        }
        if (!outputFormat.startsWith("NetCDF")
                && !outputFormat.equalsIgnoreCase("BEAM-DIMAP")
                && !outputFormat.equalsIgnoreCase("GeoTIFF")
                && !outputFormat.equalsIgnoreCase("PNG")
                && !outputFormat.equalsIgnoreCase("JPEG")) {
            throw new IllegalArgumentException("Unknown output format: " + outputFormat);
        }
        if (outputFormat.equalsIgnoreCase("NetCDF")) {
            outputFormat = "NetCDF-BEAM"; // use NetCDF with beam extensions
        }
        return outputFormat;
    }


}
