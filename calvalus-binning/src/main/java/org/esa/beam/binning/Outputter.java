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
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 */
public class Outputter {

    private String outputType;
    private String outputFormat;
    private BinnerContext binnerContext;
    private File outputFile;
    private String outputFileNameBase;
    private String outputFileNameExt;
    private double pixelSize;
    private Rectangle outputRegion;
    private Geometry roiGeometry;
    private TemporalBinSource temporalBinSource;

    public Outputter(OutputterConfig outputterConfig,
                     BinnerConfig binnerConfig,
                     Geometry roiGeometry,
                     TemporalBinSource temporalBinSource) throws Exception {
        this.roiGeometry = roiGeometry;
        this.temporalBinSource = temporalBinSource;
        outputType = outputterConfig.getOutputType();

        outputFile = new File(outputterConfig.getOutputFile());

        final String fileName = outputFile.getName();
        final int extPos = fileName.lastIndexOf(".");
        outputFileNameBase = fileName.substring(0, extPos);
        outputFileNameExt = fileName.substring(extPos + 1);

        outputFormat = outputterConfig.getOutputFormat();
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

        binnerContext = binnerConfig.createBinningContext();
        final BinManager binManager = binnerContext.getBinManager();
        final int aggregatorCount = binManager.getAggregatorCount();
        if (aggregatorCount == 0) {
            throw new IllegalArgumentException("Illegal binning context: aggregatorCount == 0");
        }

        initOutputRegion(roiGeometry);

        File parentFile = outputFile.getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }
        if (outputType.equalsIgnoreCase("Product")) {
            writeProductFile(outputterConfig, temporalBinSource);
        } else {
            writeImageFiles(outputterConfig, temporalBinSource);
        }
    }

    private void writeProductFile(OutputterConfig formatterConfig, TemporalBinSource temporalBinSource) throws Exception {
        final ProductBinRasterizer binRasterizer = createProductBinRasterizer(formatterConfig);
        output(binnerContext, outputRegion, binRasterizer, temporalBinSource);
    }

    protected void output(BinnerContext binnerContext, Rectangle outputRegion, BinRasterizer binRasterizer, TemporalBinSource temporalBinSource) throws Exception {
        BinReprojector reprojector = new BinReprojector(binnerContext, binRasterizer, outputRegion);
        final int partCount = temporalBinSource.open(this);
        reprojector.begin();
        for (int i = 0; i < partCount; i++) {
            final Iterator<? extends TemporalBin> part = temporalBinSource.getPart(i);
            reprojector.processBins(part);
            temporalBinSource.partProcessed(i, part);
        }
        reprojector.end();
        temporalBinSource.close();
    }

    private ProductBinRasterizer createProductBinRasterizer(OutputterConfig formatterConfig) throws IOException {
        final ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }

        CrsGeoCoding geoCoding;
        try {
            geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84,
                                         outputRegion.width,
                                         outputRegion.height,
                                         -180.0 + pixelSize * outputRegion.x,
                                         90.0 - pixelSize * outputRegion.y,
                                         pixelSize,
                                         pixelSize,
                                         0.0, 0.0);
        } catch (FactoryException e) {
            throw new IllegalStateException(e);
        } catch (TransformException e) {
            throw new IllegalStateException(e);
        }

        // todo - make parameter
        final String productType = "CALVALUS-L3";
        final Product product = new Product(outputFile.getName(), productType, outputRegion.width, outputRegion.height);
        product.setGeoCoding(geoCoding);
        product.setStartTime(formatterConfig.getStartTime());
        product.setEndTime(formatterConfig.getEndTime());

        // todo - add metadata
        // product.getMetadataRoot().addElement(createConfigurationMetadataElement());

        final Band numObsBand = product.addBand("num_obs", ProductData.TYPE_INT16);
        numObsBand.setNoDataValue(-1);
        numObsBand.setNoDataValueUsed(true);
        final ProductData numObsLine = numObsBand.createCompatibleRasterData(outputRegion.width, 1);

        final Band numPassesBand = product.addBand("num_passes", ProductData.TYPE_INT16);
        numPassesBand.setNoDataValue(-1);
        numPassesBand.setNoDataValueUsed(true);
        final ProductData numPassesLine = numPassesBand.createCompatibleRasterData(outputRegion.width, 1);

        String[] outputFeatureNames = binnerContext.getBinManager().getOutputFeatureNames();
        final Band[] outputBands = new Band[outputFeatureNames.length];
        final ProductData[] outputLines = new ProductData[outputFeatureNames.length];
        for (int i = 0; i < outputFeatureNames.length; i++) {
            String name = outputFeatureNames[i];
            outputBands[i] = product.addBand(name, ProductData.TYPE_FLOAT32);
            outputBands[i].setNoDataValue(binnerContext.getBinManager().getOutputFeatureFillValue(i));
            outputBands[i].setNoDataValueUsed(true);
            outputLines[i] = outputBands[i].createCompatibleRasterData(outputRegion.width, 1);
        }

        productWriter.writeProductNodes(product, outputFile);
        return new ProductBinRasterizer(productWriter,
                                        numObsBand, numObsLine,
                                        numPassesBand, numPassesLine,
                                        outputBands, outputLines);
    }

    private void initOutputRegion(Geometry roiGeometry) {
        final BinningGrid binningGrid = binnerContext.getBinningGrid();

        final int gridWidth = binningGrid.getNumRows() * 2;
        final int gridHeight = binningGrid.getNumRows();
        pixelSize = 180.0 / gridHeight;
        outputRegion = new Rectangle(gridWidth, gridHeight);
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
    }

    private void writeImageFiles(OutputterConfig outputterConfig, TemporalBinSource temporalBinSource) throws Exception {
        OutputterConfig.BandConfiguration[] bandConfigurations = outputterConfig.getBands();
        int numBands = bandConfigurations.length;
        if (numBands == 0) {
            throw new IllegalArgumentException("No output band given.");
        }
        String[] outputFeatureNames = binnerContext.getBinManager().getOutputFeatureNames();
        int[] indices = new int[numBands];
        String[] names = new String[numBands];
        float[] v1s = new float[numBands];
        float[] v2s = new float[numBands];
        for (int i = 0; i < numBands; i++) {
            OutputterConfig.BandConfiguration bandConfiguration = bandConfigurations[i];
            String nameStr = bandConfiguration.name;
            indices[i] = Integer.parseInt(bandConfiguration.index);
            names[i] = nameStr != null ? nameStr : outputFeatureNames[indices[i]];
            v1s[i] = Float.parseFloat(bandConfiguration.v1);
            v2s[i] = Float.parseFloat(bandConfiguration.v2);
        }

        final ImageBinRasterizer binRasterizer = new ImageBinRasterizer(outputRegion.width, outputRegion.height, indices);
        output(binnerContext, outputRegion, binRasterizer, temporalBinSource);

        if (outputType.equalsIgnoreCase("RGB")) {
            writeRgbImage(outputRegion.width, outputRegion.height, binRasterizer.getBandData(), v1s, v2s, outputFormat, outputFile);
        } else {
            for (int i = 0; i < numBands; i++) {
                final String fileName = String.format("%s_%s.%s", outputFileNameBase, names[i], outputFileNameExt);
                final File imageFile = new File(outputFile.getParentFile(), fileName);
                writeGrayScaleImage(outputRegion.width, outputRegion.height, binRasterizer.getBandData(i), v1s[i], v2s[i], outputFormat, imageFile);
            }
        }
    }

    private static void writeGrayScaleImage(int width, int height,
                                            float[] rawData,
                                            float rawValue1, float rawValue2,
                                            String outputFormat, File outputImageFile) throws IOException {

        final BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        final DataBufferByte dataBuffer = (DataBufferByte) image.getRaster().getDataBuffer();
        @SuppressWarnings("MismatchedReadAndWriteOfArray")
        final byte[] data = dataBuffer.getData();
        final float a = 255f / (rawValue2 - rawValue1);
        final float b = -255f * rawValue1 / (rawValue2 - rawValue1);
        for (int i = 0; i < rawData.length; i++) {
            data[i] = toByte(rawData[i], a, b);
        }
        ImageIO.write(image, outputFormat, outputImageFile);
    }

    private static void writeRgbImage(int width, int height,
                                      float[][] rawData,
                                      float[] rawValue1, float[] rawValue2,
                                      String outputFormat, File outputImageFile) throws IOException {
        final BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
        final DataBufferByte dataBuffer = (DataBufferByte) image.getRaster().getDataBuffer();
        @SuppressWarnings("MismatchedReadAndWriteOfArray")
        final byte[] data = dataBuffer.getData();
        final float[] rawDataR = rawData[0];
        final float[] rawDataG = rawData[1];
        final float[] rawDataB = rawData[2];
        final float aR = 255f / (rawValue2[0] - rawValue1[0]);
        final float bR = -255f * rawValue1[0] / (rawValue2[0] - rawValue1[0]);
        final float aG = 255f / (rawValue2[1] - rawValue1[1]);
        final float bG = -255f * rawValue1[1] / (rawValue2[1] - rawValue1[1]);
        final float aB = 255f / (rawValue2[2] - rawValue1[2]);
        final float bB = -255f * rawValue1[2] / (rawValue2[2] - rawValue1[2]);
        final int n = width * height;
        for (int i = 0, j = 0; i < n; i++, j += 3) {
            data[j + 2] = toByte(rawDataR[i], aR, bR);
            data[j + 1] = toByte(rawDataG[i], aG, bG);
            data[j] = toByte(rawDataB[i], aB, bB);
        }
        ImageIO.write(image, outputFormat, outputImageFile);
    }

    private static byte toByte(float s, float a, float b) {
        int sample = (int) (a * s + b);
        if (sample < 0) {
            sample = 0;
        } else if (sample > 255) {
            sample = 255;
        }
        return (byte) sample;
    }

}
