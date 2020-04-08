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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.bc.ceres.glevel.MultiLevelImage;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.RasterDataNode;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;
import org.esa.snap.core.util.ImageUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
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
        final MosaicConfig mosaicConfig = MosaicConfig.get(jobConfig);

        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("Mosaikking", 100);
        try {
            mosaicGrid = MosaicGrid.create(jobConfig);
            final VariableContext ctx = mosaicConfig.createVariableContext();

            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 50));
            int numTilesProcessed = 0;
            if (product != null) {
                if (product.getSceneGeoCoding() == null) {
                    throw new IllegalArgumentException("product.getGeoCoding() == null");
                }
                Geometry regionGeometry = GeometryUtils.createGeometry(jobConfig.get("calvalus.mosaic.regionGeometry"));
                numTilesProcessed = processProduct(product, regionGeometry, ctx, context, SubProgressMonitor.create(pm, 50));
                if (numTilesProcessed > 0L) {
                    context.getCounter(COUNTER_GROUP_NAME, "Input products with tiles").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME, "Tiles emitted").increment(numTilesProcessed);

//                    ProductFormatter productFormatter = new ProductFormatter("parent_of_" + context.getTaskAttemptID().toString(), DimapProductConstants.DIMAP_FORMAT_NAME, "");
//                    File productFile = productFormatter.createTemporaryProductFile();
//                    ProductIO.writeProduct(product, productFile, DimapProductConstants.DIMAP_FORMAT_NAME, false, ProgressMonitor.NULL);
//                    productFormatter.compressToHDFS(context, productFile);

                } else {
                    context.getCounter(COUNTER_GROUP_NAME, "Input products without tiles").increment(1);
                }
            } else {
                context.getCounter(COUNTER_GROUP_NAME, "Input products not-used").increment(1);
                LOG.info("Product not used");
            }
            LOG.info(MessageFormat.format("{0} stops processing of {1} ({2} tiles produced)",
                                          context.getTaskAttemptID(), processorAdapter.getInputPath(), numTilesProcessed));
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    private int processProduct(Product sourceProduct, Geometry regionGeometry, VariableContext ctx, Context mapContext, ProgressMonitor pm) throws IOException, InterruptedException {
        Geometry sourceGeometry = mosaicGrid.computeProductGeometry(sourceProduct);

        Geometry effectiveGeometry = sourceGeometry;
        if (regionGeometry != null) {
            if (sourceGeometry != null) {
                effectiveGeometry = regionGeometry.intersection(sourceGeometry);
            } else {
                effectiveGeometry = regionGeometry;
            }
            if (effectiveGeometry.isEmpty()) {
                LOG.info("Product geometry does not intersect region");
                return 0;
            }
            if (effectiveGeometry instanceof GeometryCollection) {
                LOG.info("Product geometry intersection with region does not form a proper polygon");
                return 0;
            }
        }

        final List<Point> tilePointIndices;
        if (sourceGeometry == null) {
            // if no sourceGeometry could have been calculated use other methods to find effective
            tilePointIndices = mosaicGrid.getTilePointIndicesFromProduct(sourceProduct, effectiveGeometry);
        } else {
            tilePointIndices = mosaicGrid.getTilePointIndicesFromGeometry(effectiveGeometry);
        }
        TileIndexWritable[] tileIndices = mosaicGrid.getTileIndices(tilePointIndices);

        sourceProduct.addBand(new VirtualBand("swath_x",ProductData.TYPE_INT32,
                                           sourceProduct.getSceneRasterWidth(),
                                           sourceProduct.getSceneRasterHeight(),"X"));
        sourceProduct.addBand(new VirtualBand("swath_y", ProductData.TYPE_INT32,
                                           sourceProduct.getSceneRasterWidth(),
                                           sourceProduct.getSceneRasterHeight(),"Y"));

        Product gridProduct = toPlateCareGrid(sourceProduct);
        for (int i = 0; i < ctx.getVariableCount(); i++) {
            String variableName = ctx.getVariableName(i);
            String variableExpr = ctx.getVariableExpression(i);
            if (variableExpr != null) {
                VirtualBand band = new VirtualBand(variableName,
                                                   ProductData.TYPE_FLOAT32,
                                                   gridProduct.getSceneRasterWidth(),
                                                   gridProduct.getSceneRasterHeight(),
                                                   variableExpr);
                band.setValidPixelExpression(ctx.getValidMaskExpression());
                gridProduct.addBand(band);
            }
        }

        final String maskExpr = ctx.getValidMaskExpression();
        final MultiLevelImage maskImage = gridProduct.getMaskImage(maskExpr, null); // ImageManager.getInstance().getMaskImage(maskExpr, gridProduct);

        final MultiLevelImage[] varImages = new MultiLevelImage[ctx.getVariableCount()];
        for (int i = 0; i < ctx.getVariableCount(); i++) {
            final String nodeName = ctx.getVariableName(i);
            final RasterDataNode node = getRasterDataNode(gridProduct, nodeName);
            final MultiLevelImage varImage = node.getGeophysicalImage();
            varImages[i] = varImage;
        }
        mapContext.progress();


        int numTilesTotal = tileIndices.length;
        LOG.info("Product covers #tiles : " + numTilesTotal);
        int numTilesProcessed = 0;
        TileFactory tileFactory = new TileFactory(maskImage, varImages, mapContext, mosaicGrid.getTileSize());
        pm.beginTask("Tile processing", numTilesTotal);
        int tileCounter = 0;
        for (TileIndexWritable tileIndex : tileIndices) {
            if (tileFactory.processTile(tileIndex)) {
                numTilesProcessed++;
            }
            tileCounter++;
            LOG.info(String.format("Processed %d from %d tiles (%d with data)", tileCounter, numTilesTotal, numTilesProcessed));
            pm.worked(1);
        }
        pm.done();
        return numTilesProcessed;
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
        double y = height / 2.0;

        repro.setParameter("easting", 0.0);
        repro.setParameter("northing", 0.0);
        repro.setParameter("referencePixelX", x);
        repro.setParameter("referencePixelY", y);
        repro.setParameter("width", width);
        repro.setParameter("height", height);
        repro.setParameter("noDataValue", Double.NaN);

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
            if (maskRaster == null) {
                LOG.info("Mask raster is null: " + tileIndex);
                return false;
            }
            byte[] byteBuffer = getRawMaskData(maskRaster);
            boolean containsData = containsData(byteBuffer);

            if (containsData) {
                LOG.fine("Tile contains data: " + tileIndex);
                float[][] sampleValues = new float[varImages.length][tileSize * tileSize];
                for (int i = 0; i < varImages.length; i++) {
                    Raster raster = varImages[i].getTile(tileIndex.getTileX(), tileIndex.getTileY());
                    if (raster == null) {
                        LOG.fine("Image[" + i + "] raster is null: " + tileIndex);
                        return false;
                    }
                    float[] samples = sampleValues[i];
                    raster.getPixels(raster.getMinX(), raster.getMinY(), raster.getWidth(), raster.getHeight(), samples);
                }

                TileDataWritable value = new TileDataWritable(sampleValues);
                context.write(tileIndex, value);
            } else {
                LOG.fine("Tile contains NO data: " + tileIndex);
            }
            return containsData;
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
