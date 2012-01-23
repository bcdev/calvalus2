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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.binning.*;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.ProductFactory;
import com.bc.ceres.glevel.MultiLevelImage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.datamodel.*;
import org.esa.beam.jai.ImageManager;
import org.esa.beam.util.math.MathUtils;

import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads an N1 product and produces an emits (binIndex, spatialBin) pairs.
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class L3Mapper extends Mapper<NullWritable, NullWritable, LongWritable, SpatialBin> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration jobConfig = context.getConfiguration();
        final L3Config l3Config = L3Config.get(jobConfig);
        final ProductFactory productFactory = new ProductFactory(jobConfig);

        final BinningContext ctx = l3Config.getBinningContext();
        final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);
        final SpatialBinner spatialBinner = new SpatialBinner(ctx, spatialBinEmitter);

        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(MessageFormat.format("{0} starts processing of split {1}", context.getTaskAttemptID(), split));
        final long startTime = System.nanoTime();

        Product product = productFactory.getProcessedProduct(context.getInputSplit());
        if (product != null) {
            try {
                long numObs = processProduct(product, ctx, spatialBinner, l3Config.getSuperSamplingSteps());
                if (numObs > 0L) {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product with pixels").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Pixel processed").increment(numObs);
                } else {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product without pixels").increment(1);
                }
            } finally {
                product.dispose();
            }
        } else {
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product not used").increment(1);
            LOG.info("Product not used");
        }
        productFactory.dispose();

        long stopTime = System.nanoTime();

        final Exception[] exceptions = spatialBinner.getExceptions();
        for (Exception exception : exceptions) {
            String m = MessageFormat.format("Failed to process input slice of split {0}", split);
            LOG.log(Level.SEVERE, m, exception);
        }
        // write final log entry for runtime measurements
        LOG.info(MessageFormat.format("{0} stops processing of split {1} after {2} sec ({3} observations seen, {4} bins produced)",
                                      context.getTaskAttemptID(), split, (stopTime - startTime) / 1E9, spatialBinEmitter.numObsTotal, spatialBinEmitter.numBinsTotal));
    }

    static long processProduct(Product product,
                               BinningContext ctx,
                               SpatialBinner spatialBinner,
                               float[] superSamplingSteps) {
        if (product.getGeoCoding() == null) {
            throw new IllegalArgumentException("product.getGeoCoding() == null");
        }

        final int sliceWidth = product.getSceneRasterWidth();
        for (int i = 0; i < ctx.getVariableContext().getVariableCount(); i++) {
            String variableName = ctx.getVariableContext().getVariableName(i);
            String variableExpr = ctx.getVariableContext().getVariableExpr(i);
            if (variableExpr != null) {
                VirtualBand band = new VirtualBand(variableName,
                                                   ProductData.TYPE_FLOAT32,
                                                   product.getSceneRasterWidth(),
                                                   product.getSceneRasterHeight(),
                                                   variableExpr);
                band.setValidPixelExpression(ctx.getVariableContext().getMaskExpr());
                product.addBand(band);
            }
        }

        final String maskExpr = ctx.getVariableContext().getMaskExpr();
        final MultiLevelImage maskImage = ImageManager.getInstance().getMaskImage(maskExpr, product);
        final int sliceHeight = maskImage.getTileHeight();
        boolean compatibleTileSizes = areTileSizesCompatible(maskImage, sliceWidth, sliceHeight);

        final MultiLevelImage[] varImages = new MultiLevelImage[ctx.getVariableContext().getVariableCount()];
        for (int i = 0; i < ctx.getVariableContext().getVariableCount(); i++) {
            final String nodeName = ctx.getVariableContext().getVariableName(i);
            final RasterDataNode node = getRasterDataNode(product, nodeName);
            final MultiLevelImage varImage = node.getGeophysicalImage();
            compatibleTileSizes = compatibleTileSizes && areTileSizesCompatible(varImage, sliceWidth, sliceHeight);
            varImages[i] = varImage;
        }

        final GeoCoding geoCoding = product.getGeoCoding();

        long numObsTotal = 0;
        if (compatibleTileSizes) {
            final Point[] tileIndices = maskImage.getTileIndices(null);
            for (Point tileIndex : tileIndices) {
                final ObservationSlice observationSlice = createObservationSlice(geoCoding,
                                                          maskImage, varImages,
                                                          tileIndex,
                                                          superSamplingSteps);
                spatialBinner.processObservationSlice(observationSlice);
                numObsTotal += observationSlice.getSize();
            }
        } else {
            int sceneHeight = maskImage.getHeight();
            int numSlices = MathUtils.ceilInt(sceneHeight / (double) sliceHeight);
            int currentSliceHeight = sliceHeight;
            for (int sliceIndex = 0; sliceIndex < numSlices; sliceIndex++) {
                int sliceY = sliceIndex * sliceHeight;
                if (sliceY + sliceHeight > sceneHeight) {
                    currentSliceHeight = sceneHeight - sliceY;
                }
                Rectangle sliceRect = new Rectangle(0, sliceIndex * sliceHeight, sliceWidth, currentSliceHeight);
                final Raster maskTile = maskImage.getData(sliceRect);
                final Raster[] varTiles = new Raster[varImages.length];
                for (int i = 0; i < varImages.length; i++) {
                    varTiles[i] = varImages[i].getData(sliceRect);
                }
                final ObservationSlice observationSlice = createObservationSlice(geoCoding, maskTile, varTiles, superSamplingSteps);
                spatialBinner.processObservationSlice(observationSlice);
                numObsTotal += observationSlice.getSize();
            }
        }

        spatialBinner.complete();
        return numObsTotal;
    }

    private static ObservationSlice createObservationSlice(GeoCoding geoCoding,
                                                           RenderedImage maskImage,
                                                           RenderedImage[] varImages,
                                                           Point tileIndex,
                                                           float[] superSamplingSteps) {
        final Raster maskTile = maskImage.getTile(tileIndex.x, tileIndex.y);
        final Raster[] varTiles = new Raster[varImages.length];
        for (int i = 0; i < varImages.length; i++) {
            varTiles[i] = varImages[i].getTile(tileIndex.x, tileIndex.y);
        }
        return createObservationSlice(geoCoding, maskTile, varTiles, superSamplingSteps);
    }

    private static ObservationSlice createObservationSlice(GeoCoding geoCoding, Raster maskTile, Raster[] varTiles, float[] superSamplingSteps) {
        final ObservationSlice observationSlice = new ObservationSlice(varTiles, maskTile.getWidth() * maskTile.getHeight());
        final int y1 = maskTile.getMinY();
        final int y2 = y1 + maskTile.getHeight();
        final int x1 = maskTile.getMinX();
        final int x2 = x1 + maskTile.getWidth();
        final PixelPos pixelPos = new PixelPos();
        final GeoPos geoPos = new GeoPos();
        for (int y = y1; y < y2; y++) {
            for (int x = x1; x < x2; x++) {
                if (maskTile.getSample(x, y, 0) != 0) {
                    final float[] samples = observationSlice.createObservationSamples(x, y);
                    for (float dy : superSamplingSteps) {
                        for (float dx : superSamplingSteps) {
                            pixelPos.setLocation(x + dx, y + dy);
                            geoCoding.getGeoPos(pixelPos, geoPos);
                            observationSlice.addObservation(geoPos.lat, geoPos.lon, samples);
                        }
                    }
                }
            }
        }
        return observationSlice;
    }

    private static RasterDataNode getRasterDataNode(Product product, String nodeName) {
        final RasterDataNode node = product.getRasterDataNode(nodeName);
        if (node == null) {
            throw new IllegalStateException(String.format("Can't find raster data node '%s' in product '%s'",
                                                          nodeName, product.getName()));
        }
        return node;
    }

    private static boolean areTileSizesCompatible(MultiLevelImage image, int sliceWidth, int sliceHeight) {
        return image.getTileWidth() == sliceWidth && image.getTileHeight() == sliceHeight;
    }

    private static class SpatialBinEmitter implements SpatialBinProcessor {
        private Context context;
        int numObsTotal = 0;
        int numBinsTotal = 0;

        public SpatialBinEmitter(Context context) {
            this.context = context;
        }

        @Override
        public void processSpatialBinSlice(BinningContext ctx, List<SpatialBin> spatialBins) throws Exception {
            for (SpatialBin spatialBin : spatialBins) {
                context.write(new LongWritable(spatialBin.getIndex()), spatialBin);
                numObsTotal += spatialBin.getNumObs();
                numBinsTotal++;
            }
        }
    }

}
