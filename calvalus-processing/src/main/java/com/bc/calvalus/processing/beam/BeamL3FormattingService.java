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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.binning.Aggregator;
import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.binning.WritableVector;
import com.bc.calvalus.processing.shellexec.XmlDoc;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.io.IOUtils;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import org.xml.sax.SAXException;

import javax.imageio.ImageIO;
import javax.xml.parsers.ParserConfigurationException;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 */
public class BeamL3FormattingService {

    private String outputType;
    private String outputFormat;
    private Path l3OutputDir;
    private BinningContext binningContext;
    private File outputFile;
    private String outputFileNameBase;
    private String outputFileNameExt;
    private double pixelSize;
    private Rectangle outputRegion;

    private final Logger logger;
    private final Configuration configuration;

    public BeamL3FormattingService(Logger logger, Configuration configuration) {
        this.logger = logger;
        this.configuration = configuration;
    }

    public int format(String requestContent) throws Exception {
        WpsConfig wpsConfig = new WpsConfig(requestContent);
        FormatterL3Config formatterL3Config = FormatterL3Config.create(wpsConfig.getRequestXmlDoc());
        return format(formatterL3Config, wpsConfig.getRequestOutputDir());
    }

    public int format(FormatterL3Config formatterL3Config, String jobOutputDir) throws Exception {
        outputType = formatterL3Config.getOutputType();

        outputFile = new File(formatterL3Config.getOutputFile());
        final String fileName = outputFile.getName();
        final int extPos = fileName.lastIndexOf(".");
        outputFileNameBase = fileName.substring(0, extPos);
        outputFileNameExt = fileName.substring(extPos + 1);

        outputFormat = formatterL3Config.getOutputFormat();
        if (outputFormat == null) {
            outputFormat = outputFileNameExt.equalsIgnoreCase("nc") ? "NetCDF"
                            : outputFileNameExt.equalsIgnoreCase("dim") ? "BEAM-DIMAP"
                            : outputFileNameExt.equalsIgnoreCase("png") ? "PNG"
                            : outputFileNameExt.equalsIgnoreCase("jpg") ? "JPEG" : null;
        }
        if (outputFormat == null) {
            throw new IllegalArgumentException("No output format given");
        }
        if (!outputFormat.equalsIgnoreCase("NetCDF")
                && !outputFormat.equalsIgnoreCase("BEAM-DIMAP")
                && !outputFormat.equalsIgnoreCase("PNG")
                && !outputFormat.equalsIgnoreCase("JPEG")) {
            throw new IllegalArgumentException("Unknown output format: " + outputFormat);
        }

        l3OutputDir = new Path(jobOutputDir);
        BeamL3Config l3Config = readL3Config();

        binningContext = l3Config.getBinningContext();
        final BinManager binManager = binningContext.getBinManager();
        final int aggregatorCount = binManager.getAggregatorCount();
        if (aggregatorCount == 0) {
            throw new IllegalArgumentException("Illegal binning context: aggregatorCount == 0");
        }

        logger.info("aggregators.length = " + aggregatorCount);
        for (int i = 0; i < aggregatorCount; i++) {
            Aggregator aggregator = binManager.getAggregator(i);
            logger.info("aggregators." + i + " = " + aggregator);
        }

        computeOutputRegion(l3Config);

        if (outputType.equalsIgnoreCase("Product")) {
            writeProductFile(formatterL3Config.getStartTime(), formatterL3Config.getEndTime());
        } else {
            writeImageFiles(formatterL3Config);
        }

        return 0;
    }

    private BeamL3Config readL3Config() throws IOException, SAXException, ParserConfigurationException {
        FileSystem fs = l3OutputDir.getFileSystem(configuration);
        InputStream is = fs.open(new Path(l3OutputDir, BeamL3Config.L3_REQUEST_FILENAME));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(is, baos);
        String wpsContent = baos.toString();
        return BeamL3Config.create(new XmlDoc(wpsContent));
    }

    private void computeOutputRegion(BeamL3Config l3Config) {
        final Geometry roiGeometry = l3Config.getRegionOfInterest();
        final BinningGrid binningGrid = binningContext.getBinningGrid();

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
            logger.info("unclippedOutputRegion = " + unclippedOutputRegion);
            outputRegion = unclippedOutputRegion.intersection(outputRegion);
        }
        logger.info("outputRegion = " + outputRegion);
    }

    private void writeProductFile(ProductData.UTC startTime, ProductData.UTC endTime) throws Exception {
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
        final Product product = new Product(outputFile.getName(), "CALVALUS-L3", outputRegion.width, outputRegion.height);
        product.setGeoCoding(geoCoding);
        product.setStartTime(startTime);
        product.setEndTime(endTime);

        final Band numObsBand = product.addBand("num_obs", ProductData.TYPE_INT16);
        numObsBand.setNoDataValue(-1);
        numObsBand.setNoDataValueUsed(true);
        final ProductData numObsLine = numObsBand.createCompatibleRasterData(outputRegion.width, 1);

        final Band numPassesBand = product.addBand("num_passes", ProductData.TYPE_INT16);
        numPassesBand.setNoDataValue(-1);
        numPassesBand.setNoDataValueUsed(true);
        final ProductData numPassesLine = numPassesBand.createCompatibleRasterData(outputRegion.width, 1);

        int outputPropertyCount = binningContext.getBinManager().getOutputPropertyCount();
        final Band[] outputBands = new Band[outputPropertyCount];
        final ProductData[] outputLines = new ProductData[outputPropertyCount];
        for (int i = 0; i < outputPropertyCount; i++) {
            String name = binningContext.getBinManager().getOutputPropertyName(i);
            outputBands[i] = product.addBand(name, ProductData.TYPE_FLOAT32);
            outputBands[i].setNoDataValue(Double.NaN);
            outputBands[i].setNoDataValueUsed(true);
            outputLines[i] = outputBands[i].createCompatibleRasterData(outputRegion.width, 1);
        }

        productWriter.writeProductNodes(product, outputFile);
        final ProductDataWriter dataWriter = new ProductDataWriter(productWriter,
                                                                   numObsBand, numObsLine,
                                                                   numPassesBand, numPassesLine,
                                                                   outputBands, outputLines);
        L3Reprojector.reproject(configuration, binningContext, outputRegion, l3OutputDir, dataWriter);
        productWriter.close();
    }

    private void writeImageFiles(FormatterL3Config formatterL3Config) throws Exception {
        FormatterL3Config.BandConfiguration[] bandConfigurations = formatterL3Config.getBands();
        int numBands = bandConfigurations.length;
        if (numBands == 0) {
            throw new IllegalArgumentException("No output band given.");
        }
        int[] indices = new int[numBands];
        String[] names = new String[numBands];
        float[] v1s = new float[numBands];
        float[] v2s = new float[numBands];
        for (int i = 0; i < numBands; i++) {
            FormatterL3Config.BandConfiguration bandConfiguration = bandConfigurations[i];
            String nameStr = bandConfiguration.name;

            indices[i] = Integer.parseInt(bandConfiguration.index);
            names[i] = nameStr != null ? nameStr : binningContext.getBinManager().getOutputPropertyName(indices[i]);
            v1s[i] = Float.parseFloat(bandConfiguration.v1);
            v2s[i] = Float.parseFloat(bandConfiguration.v2);
        }

        final ImageRaster raster = new ImageRaster(outputRegion.width, outputRegion.height, indices);
        L3Reprojector.reproject(configuration, binningContext, outputRegion, l3OutputDir, raster);

        if (outputType.equalsIgnoreCase("RGB")) {
            writeRgbImage(outputRegion.width, outputRegion.height, raster.getBandData(), v1s, v2s, outputFormat, outputFile);
        } else {
            for (int i = 0; i < numBands; i++) {
                final String fileName = String.format("%s_%s.%s", outputFileNameBase, names[i], outputFileNameExt);
                final File imageFile = new File(outputFile.getParentFile(), fileName);
                writeGrayScaleImage(outputRegion.width, outputRegion.height, raster.getBandData(i), v1s[i], v2s[i], outputFormat, imageFile);
            }
        }
    }


    private void writeGrayScaleImage(int width, int height,
                                            float[] rawData,
                                            float rawValue1, float rawValue2,
                                            String outputFormat, File outputImageFile) throws IOException {

        logger.info(MessageFormat.format("writing image {0}", outputImageFile));
        final BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        final DataBufferByte dataBuffer = (DataBufferByte) image.getRaster().getDataBuffer();
        final byte[] data = dataBuffer.getData();
        final float a = 255f / (rawValue2 - rawValue1);
        final float b = -255f * rawValue1 / (rawValue2 - rawValue1);
        for (int i = 0; i < rawData.length; i++) {
            data[i] = toByte(rawData[i], a, b);
        }
        ImageIO.write(image, outputFormat, outputImageFile);
    }

    private void writeRgbImage(int width, int height,
                                      float[][] rawData,
                                      float[] rawValue1, float[] rawValue2,
                                      String outputFormat, File outputImageFile) throws IOException {
        logger.info(MessageFormat.format("writing image {0}", outputImageFile));
        final BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
        final DataBufferByte dataBuffer = (DataBufferByte) image.getRaster().getDataBuffer();
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

    private final static class ImageRaster extends L3Reprojector.TemporalBinProcessor {
        private final int rasterWidth;
        private final int[] bandIndices;
        private final float[][] bandData;

        private final int bandCount;


        public ImageRaster(int rasterWidth, int rasterHeight, int[] bandIndices) {
            this.rasterWidth = rasterWidth;
            this.bandIndices = bandIndices.clone();
            this.bandCount = bandIndices.length;
            this.bandData = new float[bandCount][rasterWidth * rasterHeight];
            for (int i = 0; i < bandCount; i++) {
                Arrays.fill(bandData[i], Float.NaN);
            }
        }

        public float[][] getBandData() {
            return bandData;
        }

        public float[] getBandData(int bandIndex) {
            return this.bandData[bandIndex];
        }

        @Override
        public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) {
            for (int i = 0; i < bandCount; i++) {
                bandData[i][rasterWidth * y + x] = outputVector.get(bandIndices[i]);
            }
        }

        @Override
        public void processMissingBin(int x, int y) throws Exception {
            for (int i = 0; i < bandCount; i++) {
                bandData[i][rasterWidth * y + x] = Float.NaN;
            }
        }
    }

    private final static class ProductDataWriter extends L3Reprojector.TemporalBinProcessor {
        private final int width;
        private final ProductData numObsLine;
        private final ProductData numPassesLine;
        private final Band[] outputBands;
        private final ProductData[] outputLines;
        private final ProductWriter productWriter;
        private final Band numObsBand;
        private final Band numPassesBand;
        int yLast;

        public ProductDataWriter(ProductWriter productWriter, Band numObsBand, ProductData numObsLine, Band numPassesBand, ProductData numPassesLine, Band[] outputBands, ProductData[] outputLines) {
            this.numObsLine = numObsLine;
            this.numPassesLine = numPassesLine;
            this.outputBands = outputBands;
            this.outputLines = outputLines;
            this.productWriter = productWriter;
            this.numObsBand = numObsBand;
            this.numPassesBand = numPassesBand;
            this.width = numObsBand.getSceneRasterWidth();
            this.yLast = 0;
            initLine();
        }

        @Override
        public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception {
            setData(x, temporalBin, outputVector);
            if (y != yLast) {
                completeLine();
                yLast = y;
            }
        }

        @Override
        public void processMissingBin(int x, int y) throws Exception {
            setNoData(x);
            if (y != yLast) {
                completeLine();
                yLast = y;
            }
        }

        @Override
        public void end(BinningContext ctx) throws Exception {
            completeLine();
        }

        private void completeLine() throws IOException {
            writeLine(yLast);
            initLine();
        }

        private void writeLine(int y) throws IOException {
            productWriter.writeBandRasterData(numObsBand, 0, y, width, 1, numObsLine, ProgressMonitor.NULL);
            productWriter.writeBandRasterData(numPassesBand, 0, y, width, 1, numPassesLine, ProgressMonitor.NULL);
            for (int i = 0; i < outputBands.length; i++) {
                productWriter.writeBandRasterData(outputBands[i], 0, y, width, 1, outputLines[i], ProgressMonitor.NULL);
            }
        }

        private void initLine() {
            for (int x = 0; x < width; x++) {
                setNoData(x);
            }
        }

        private void setData(int x, TemporalBin temporalBin, WritableVector outputVector) {
            numObsLine.setElemIntAt(x, temporalBin.getNumObs());
            numPassesLine.setElemIntAt(x, temporalBin.getNumPasses());
            for (int i = 0; i < outputBands.length; i++) {
                outputLines[i].setElemFloatAt(x, outputVector.get(i));
            }
        }

        private void setNoData(int x) {
            numObsLine.setElemIntAt(x, -1);
            numPassesLine.setElemIntAt(x, -1);
            for (int i = 0; i < outputBands.length; i++) {
                outputLines[i].setElemFloatAt(x, Float.NaN);
            }
        }

    }
}
