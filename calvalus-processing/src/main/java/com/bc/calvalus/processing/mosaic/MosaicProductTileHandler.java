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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.awt.Point;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;

/**
 * Writes a series of products base on the tile data it receives.
 *
 * @author MarcoZ
 */
public class MosaicProductTileHandler extends MosaicTileHandler {

    private final TaskInputOutputContext<?, ?, ?, ?> context;
    private final String outputPrefix;
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
        String outputPrefix = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, null);

        MosaicGrid mosaicGrid = MosaicGrid.create(jobConfig);

        MosaicAlgorithm algorithm = MosaicUtils.createAlgorithm(jobConfig);

        return new MosaicProductTileHandler(context, mosaicGrid, outputPrefix, algorithm, format, compression);
    }

    MosaicProductTileHandler(TaskInputOutputContext<?, ?, ?, ?> context, MosaicGrid mosaicGrid, String outputPrefix, MosaicAlgorithm algorithm, String format, String compression) {
        super(mosaicGrid);
        this.context = context;
        this.outputPrefix = outputPrefix;
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
                int dataType = bands[bandIndex].getDataType();
                float[] floatSamples = samples[bandIndex];
                if (dataType == ProductData.TYPE_FLOAT32) {
                    productData[bandIndex] = ProductData.createInstance(floatSamples);
                } else {
                    ProductData pdata = ProductData.createInstance(dataType, floatSamples.length);
                    for (int i = 0; i < floatSamples.length; i++) {
                        pdata.setElemFloatAt(i, floatSamples[i]);
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
    protected void closeProduct() throws IOException {
        if (product != null) {
            try {
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
        String productName = getTileProductName(outputPrefix, macroTile.x, macroTile.y);
        MosaicProductFactory productFactory = algorithm.getProductFactory();
        product = productFactory.createProduct(productName, productRect);
        CrsGeoCoding geoCoding = getMosaicGrid().createMacroCRS(macroTile);
        product.setGeoCoding(geoCoding);

        productFormatter = new ProductFormatter(productName, format, compression);
        productFile = productFormatter.createTemporaryProductFile();
        productWriter = createProductWriter(product, productFile, productFormatter.getOutputFormat());
    }

    static String getTileProductName(String prefix, int tileX, int tileY) {
        return String.format("%s-v%02dh%02d", prefix, tileY, tileX);
    }

    static ProductWriter createProductWriter(Product product, File outputFile, String outputFormat) throws IOException {

        ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }
        productWriter.writeProductNodes(product, outputFile);
        return productWriter;
    }
}
