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

import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.beam.ProductFactory;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.ceres.glevel.MultiLevelImage;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.datamodel.RasterDataNode;
import org.esa.beam.framework.datamodel.VirtualBand;
import org.esa.beam.gpf.operators.standard.reproject.ReprojectionOp;
import org.esa.beam.jai.ImageManager;
import org.esa.beam.util.ImageUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Logger;

/**
 * Reads an N1 product and emits (tileIndex, tileData) pairs.
 *
 * @author Marco Zuehlke
 */
public class MosaicMapper extends Mapper<NullWritable, NullWritable, TileIndexWritable, TileDataWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME = "Mosaic";

    private MosaicGrid mosaicGrid;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration jobConfig = context.getConfiguration();
        final L3Config l3Config = L3Config.get(jobConfig);
        final ProductFactory productFactory = new ProductFactory(jobConfig);
        mosaicGrid = MosaicGrid.create(jobConfig);
        final VariableContext ctx = l3Config.getVariableContext();
        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(MessageFormat.format("{0} starts processing of split {1}", context.getTaskAttemptID(), split));
        final long startTime = System.nanoTime();

        Path inputPath = split.getPath();
        String inputFormat = jobConfig.get(JobConfigNames.CALVALUS_INPUT_FORMAT, null);
        Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        String level2OperatorName = jobConfig.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String level2Parameters = jobConfig.get(JobConfigNames.CALVALUS_L2_PARAMETERS);
        Product product = productFactory.getProduct(inputPath,
                                                    inputFormat,
                                                    regionGeometry,
                                                    true,
                                                    level2OperatorName,
                                                    level2Parameters);
        int numTiles = 0;
        if (product != null) {
            try {
                if (product.getGeoCoding() == null) {
                    throw new IllegalArgumentException("product.getGeoCoding() == null");
                }
                numTiles = processProduct(product, regionGeometry, ctx, context);
                if (numTiles > 0L) {
                    context.getCounter(COUNTER_GROUP_NAME, "Input products with tiles").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME, "Tiles emitted").increment(numTiles);
                } else {
                    context.getCounter(COUNTER_GROUP_NAME, "Input products without tiles").increment(1);
                }
            } finally {
                product.dispose();
            }
        } else {
            context.getCounter(COUNTER_GROUP_NAME, "Input products not-used").increment(1);
            LOG.info("Product not used");
        }

        long stopTime = System.nanoTime();

        // write final log entry for runtime measurements
        LOG.info(MessageFormat.format("{0} stops processing of split {1} after {2} sec ({3} tiless produced)",
                                      context.getTaskAttemptID(), split, (stopTime - startTime) / 1E9, numTiles));
    }

    private int processProduct(Product sourceProduct, Geometry regionGeometry, VariableContext ctx, Context context) throws IOException, InterruptedException {
        Geometry sourceGeometry = mosaicGrid.computeProductGeometry(sourceProduct);
        if (sourceGeometry == null || sourceGeometry.isEmpty()) {
            LOG.info("Product geometry is empty");
            return 0;
        }
        if (regionGeometry != null) {
            sourceGeometry = regionGeometry.intersection(sourceGeometry);
            if (sourceGeometry.isEmpty()) {
                LOG.info("Product geometry does not intersect region");
                return 0;
            }
        }
        Product gridProduct = toPlateCareGrid(sourceProduct);
        for (int i = 0; i < ctx.getVariableCount(); i++) {
            String variableName = ctx.getVariableName(i);
            String variableExpr = ctx.getVariableExpr(i);
            if (variableExpr != null) {
                VirtualBand band = new VirtualBand(variableName,
                                                   ProductData.TYPE_FLOAT32,
                                                   gridProduct.getSceneRasterWidth(),
                                                   gridProduct.getSceneRasterHeight(),
                                                   variableExpr);
                band.setValidPixelExpression(ctx.getMaskExpr());
                gridProduct.addBand(band);
            }
        }

        final String maskExpr = ctx.getMaskExpr();
        final MultiLevelImage maskImage = ImageManager.getInstance().getMaskImage(maskExpr, gridProduct);

        final MultiLevelImage[] varImages = new MultiLevelImage[ctx.getVariableCount()];
        for (int i = 0; i < ctx.getVariableCount(); i++) {
            final String nodeName = ctx.getVariableName(i);
            final RasterDataNode node = getRasterDataNode(gridProduct, nodeName);
            final MultiLevelImage varImage = node.getGeophysicalImage();
            varImages[i] = varImage;
        }
        TileIndexWritable[] tileIndices = mosaicGrid.getTileIndices(sourceGeometry);
        LOG.info("Product covers #tiles : " + tileIndices.length);
        int numTileTotal = 0;
        TileFactory tileFactory = new TileFactory(maskImage, varImages, context, mosaicGrid.getTileSize());
        for (TileIndexWritable tileIndex : tileIndices) {
            LOG.info("Processing tile: " + tileIndex);
            context.progress();
            if (tileFactory.processTile(tileIndex)) {
                numTileTotal++;
            }
        }
        return numTileTotal;
    }


    private static RasterDataNode getRasterDataNode(Product product, String nodeName) {
        final RasterDataNode node = product.getRasterDataNode(nodeName);
        if (node == null) {
            throw new IllegalStateException(String.format("Can't find raster data node '%s' in product '%s'",
                                                          nodeName, product.getName()));
        }
        return node;
    }

    private Product toPlateCareGrid(Product sourceProduct) {
        final ReprojectionOp repro = new ReprojectionOp();

        double pixelSize = mosaicGrid.getPixelSize();
        int tileSize = mosaicGrid.getTileSize();

        repro.setParameter("resampling", "Nearest");
        repro.setParameter("includeTiePointGrids", true);
        repro.setParameter("orientation", 0.0);
        repro.setParameter("pixelSizeX", pixelSize);
        repro.setParameter("pixelSizeY", pixelSize);
        repro.setParameter("tileSizeX", tileSize);
        repro.setParameter("tileSizeY", tileSize);
        repro.setParameter("crs", DefaultGeographicCRS.WGS84.toString());

        Rectangle rectangle = mosaicGrid.computeBounds(null);
        int width = rectangle.width;
        int height = rectangle.height;
        double x = width / 2.0;
        double y = height / 2;

        repro.setParameter("easting", 0.0);
        repro.setParameter("northing", 0.0);
        repro.setParameter("referencePixelX", x);
        repro.setParameter("referencePixelY", y);
        repro.setParameter("width", width);
        repro.setParameter("height", height);

        repro.setSourceProduct(sourceProduct);
        return repro.getTargetProduct();
    }


    private static class TileFactory {

        private final MultiLevelImage maskImage;
        private final MultiLevelImage[] varImages;
        private final Context context;
        private final int tileSize;

        public TileFactory(MultiLevelImage maskImage, MultiLevelImage[] varImages, Context context, int tileSize) {
            this.maskImage = maskImage;
            this.varImages = varImages;
            this.context = context;
            this.tileSize = tileSize;
        }

        private boolean processTile(TileIndexWritable tileIndex) throws IOException, InterruptedException {
            Raster maskRaster = maskImage.getTile(tileIndex.getTileX(), tileIndex.getTileY());
            byte[] byteBuffer = getRawMaskData(maskRaster);
            boolean containsData = containsData(byteBuffer);

            if (containsData) {
                LOG.info("Tile contains data: " + tileIndex);
                float[][] sampleValues = new float[varImages.length][tileSize * tileSize];
                for (int i = 0; i < varImages.length; i++) {
                    Raster raster = varImages[i].getTile(tileIndex.getTileX(), tileIndex.getTileY());
                    float[] samples = sampleValues[i];
                    raster.getPixels(raster.getMinX(), raster.getMinY(), raster.getWidth(), raster.getHeight(), samples);
                }

                TileDataWritable value = new TileDataWritable(sampleValues);
                context.write(tileIndex, value);
                return true;
            }
            return false;
        }

        private static byte[] getRawMaskData(Raster mask) {
            DataBuffer dataBuffer = mask.getDataBuffer();
            Object primitiveArray = ImageUtils.getPrimitiveArray(dataBuffer);
            byte[] byteBuffer = (primitiveArray instanceof byte[]) ? (byte[]) primitiveArray : null;
            if (byteBuffer == null) {
                throw new IllegalStateException("mask is not of type byte");
            }
            return byteBuffer;
        }

        private static boolean containsData(byte[] byteBuffer) {
            for (int sample : byteBuffer) {
                if (sample != 0) {
                    return true;
                }
            }
            return false;
        }

    }
}
