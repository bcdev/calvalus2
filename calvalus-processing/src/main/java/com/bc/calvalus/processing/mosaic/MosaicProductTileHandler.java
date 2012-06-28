/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.metadata.MetadataEngine;
import com.bc.ceres.metadata.SimpleFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.velocity.VelocityContext;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.io.FileUtils;

import java.awt.Point;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Date;

/**
 * Writes a series of products base on the tile data it receives.
 *
 * @author MarcoZ
 */
public class MosaicProductTileHandler extends MosaicTileHandler {

    private final TaskInputOutputContext<?, ?, ?, ?> context;
    private final String outputNameFormat;
    private final MosaicAlgorithm algorithm;
    private final String format;
    private final String compression;

    private ProductWriter productWriter;
    private ProductFormatter productFormatter;
    private Product product;
    private ProductData[] NO_DATA_SAMPLES;
    private File productFile;

    public static MosaicTileHandler createHandler(TaskInputOutputContext<?, ?, ?, ?> context) {
        Configuration jobConfig = context.getConfiguration();

        String format = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
        String compression = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, null);
        String outputNameFormat = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_NAMEFORMAT, null);

        MosaicGrid mosaicGrid = MosaicGrid.create(jobConfig);

        MosaicAlgorithm algorithm = MosaicUtils.createAlgorithm(jobConfig);

        return new MosaicProductTileHandler(context, mosaicGrid, outputNameFormat, algorithm, format, compression);
    }

    MosaicProductTileHandler(TaskInputOutputContext<?, ?, ?, ?> context, MosaicGrid mosaicGrid, String outputNameFormat, MosaicAlgorithm algorithm, String format, String compression) {
        super(mosaicGrid);
        this.context = context;
        this.outputNameFormat = outputNameFormat;
        this.algorithm = algorithm;
        this.format = format;
        this.compression = compression;
    }

    @Override
    protected void writeDataTile(Point tile, TileDataWritable data) throws IOException {
        if (product != null) {
            float[][] samples = data.getSamples();
            Band[] bands = product.getBands();
            ProductData[] productData = new ProductData[bands.length];
            for (int bandIndex = 0; bandIndex < bands.length; bandIndex++) {
                Band band = bands[bandIndex];
                int dataType = band.getDataType();

                float[] floatSamples = samples[bandIndex];
                if (dataType == ProductData.TYPE_FLOAT32) {
                    productData[bandIndex] = ProductData.createInstance(floatSamples);
                } else {
                    ProductData pdata = ProductData.createInstance(dataType, floatSamples.length);
                    if (band.isScalingApplied()) {
                        for (int i = 0; i < floatSamples.length; i++) {
                            pdata.setElemDoubleAt(i, band.scaleInverse(floatSamples[i]));
                        }
                    } else {
                        for (int i = 0; i < floatSamples.length; i++) {
                            pdata.setElemDoubleAt(i, floatSamples[i]);
                        }
                    }
                    productData[bandIndex] = pdata;
                }
            }
            write(tile, productData);
        }
    }

    @Override
    protected void writeNaNTile(Point tile) throws IOException {
        write(tile, getNoDataSamples());
    }

    private void write(Point tile, ProductData[] samples) throws IOException {
        Band[] bands = product.getBands();
        Rectangle tileRect = getMosaicGrid().getTileRectangle(tile.x, tile.y);
        for (int i = 0; i < bands.length; i++) {
            context.progress();
            ProductData productData = samples[i];
            productWriter.writeBandRasterData(bands[i], tileRect.x, tileRect.y, tileRect.width, tileRect.height,
                                              productData, ProgressMonitor.NULL);
        }
    }

    private ProductData[] getNoDataSamples() {
        if (NO_DATA_SAMPLES == null && product != null) {
            int tileSize = getMosaicGrid().getTileSize();
            int numElems = tileSize * tileSize;
            NO_DATA_SAMPLES = new ProductData[product.getNumBands()];//[tileSize * tileSize];
            for (int bandIndex = 0; bandIndex < NO_DATA_SAMPLES.length; bandIndex++) {
                Band band = product.getBandAt(bandIndex);
                float noDataValue = (float) band.getNoDataValue();
                int dataType = band.getDataType();
                ProductData pdata = ProductData.createInstance(dataType, numElems);
                for (int i = 0; i < numElems; i++) {
                    pdata.setElemFloatAt(i, noDataValue);
                }
                NO_DATA_SAMPLES[bandIndex] = pdata;
            }
        }
        return NO_DATA_SAMPLES;
    }

    @Override
    protected void finishProduct() throws IOException {
        if (product != null) {
            try {
                writeMetadata();
                productWriter.close();
                productFormatter.compressToHDFS(context, productFile);
                context.getCounter("Mosaic", "Products written").increment(1);
            } finally {
                product.dispose();
                product = null;
                productFormatter.cleanupTempDir();
                productFormatter = null;
            }
        }
    }

    @Override
    protected void createProduct(Point macroTile) throws IOException {
        Rectangle productRect = getMosaicGrid().getMacroTileRectangle(macroTile.x, macroTile.y);
        String productName = getTileProductName(outputNameFormat, macroTile.x, macroTile.y);
        MosaicProductFactory productFactory = algorithm.getProductFactory();
        product = productFactory.createProduct(productName, productRect);
        CrsGeoCoding geoCoding = getMosaicGrid().createMacroCRS(macroTile);
        product.setGeoCoding(geoCoding);

        productFormatter = new ProductFormatter(productName, format, compression);
        productFile = productFormatter.createTemporaryProductFile();
        productWriter = createProductWriter(product, productFile, productFormatter.getOutputFormat());
    }

    private void writeMetadata() throws IOException {
        try {
            Configuration configuration = context.getConfiguration();
                String templatePath = configuration.get("calvalus.metadata.template");
            if (templatePath != null) {
                MetadataEngine metadataEngine = new MetadataEngine(new HDFSSimpleFileSystem(context));
                String metadataPath = configuration.get("calvalus.metadata.URL");
                if (metadataPath != null) {
                    metadataEngine.readMetadata("metadata", metadataPath, true);
                }
                VelocityContext velocityContext = metadataEngine.getVelocityContext();

                velocityContext.put("processingTime", ProductData.UTC.create(new Date(), 0));

                velocityContext.put("targetBaseName", FileUtils.getFilenameWithoutExtension(productFormatter.getProductFilename()));
                velocityContext.put("targetName", productFormatter.getProductFilename());
                velocityContext.put("targetFormat", productFormatter.getOutputFormat());

                Point currentMacroTile = getCurrentMacroTile();
                velocityContext.put("tileX", currentMacroTile.x);
                velocityContext.put("tileY", currentMacroTile.y);

                velocityContext.put("product", product);

                String productFilename = productFormatter.getProductFilename();
                metadataEngine.writeTargetMetadata(templatePath, productFilename);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    static String getTileProductName(String outputNameFormat, int tileX, int tileY) {
        return String.format(outputNameFormat, tileY, tileX);
    }

    static ProductWriter createProductWriter(Product product, File outputFile, String outputFormat) throws IOException {

        ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }
        productWriter.writeProductNodes(product, outputFile);
        return productWriter;
    }

    private static class HDFSSimpleFileSystem implements SimpleFileSystem {

        private final FileSystem fileSystem;
        private final TaskInputOutputContext<?, ?, ?, ?> context;

        public HDFSSimpleFileSystem(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
            this.context = context;
            Configuration configuration = context.getConfiguration();
            fileSystem = FileSystem.get(configuration);
        }

        @Override
        public Reader getReader(String path) throws IOException {
            FSDataInputStream inputStream = fileSystem.open(new Path(path));
            return new InputStreamReader(inputStream);
        }

        @Override
        public Writer getWriter(String path) throws IOException {
            Path workOutputPath;
            try {
                workOutputPath = FileOutputFormat.getWorkOutputPath(context);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            Path workPath = new Path(workOutputPath, path);
            FSDataOutputStream fsDataOutputStream = fileSystem.create(workPath);
            return new OutputStreamWriter(fsDataOutputStream);
        }

        @Override
        public String[] list(String path) throws IOException {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
            if (fileStatuses == null) {
                return null;
            }
            String[] names = new String[fileStatuses.length];
            for (int i = 0; i < fileStatuses.length; i++) {
                names[i] = fileStatuses[i].getPath().getName();
            }
            return names;
        }
    }
}
