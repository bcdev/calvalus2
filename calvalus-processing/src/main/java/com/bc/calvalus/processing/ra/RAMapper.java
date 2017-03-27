/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.core.util.StringUtils;

import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.Raster;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * The mapper for the region analysis workflow
 *
 * @author MarcoZ
 */
public class RAMapper extends Mapper<NullWritable, NullWritable, RAKey, RAValue> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        context.progress();

        final Configuration jobConfig = context.getConfiguration();
        final RAConfig raConfig = RAConfig.get(jobConfig);

        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        int numRegions = raConfig.getNumRegions();
        pm.beginTask("Region Analysis", numRegions * 2);
        try {
            ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, numRegions));
            if (product != null) {
                Extractor extractor = new Extractor(product, raConfig);
                Iterator<NamedRegion> regionIterator = raConfig.createNamedRegionIterator(context.getConfiguration());
                int regionIndex = 0;
                while (regionIterator.hasNext()) {
                    NamedRegion namedRegion = regionIterator.next();
                    ProgressMonitor subPM = SubProgressMonitor.create(pm, 1);
                    Extract extract = extractor.performExtraction(namedRegion.name, namedRegion.geometry, subPM);
                    if (extract != null) {
                        RAKey key = new RAKey(regionIndex, namedRegion.name, extract.time);
                        RAValue value = new RAValue(extract.numPixel, extract.samples, extract.time, product.getName());
                        context.write(key, value);
                    }
                    regionIndex++;
                }
            } else {
                LOG.warning("product does not cover region, skipping processing.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
            }
        } finally {
            pm.done();
        }
    }

    static class Extractor {

        private final Product product;
        private final PlanarImage maskImage;
        private final PlanarImage[] dataImages;
        private final boolean equalTileGrids;

        public Extractor(Product product, RAConfig raConfig) {
            this.product = product;

            if (product.getSceneTimeCoding() == null && product.getStartTime() == null && product.getEndTime() == null) {
                throw new IllegalArgumentException("Product has no time information");
            }

            String expression = StringUtils.isNotNullAndNotEmpty(raConfig.getValidExpressions()) ? raConfig.getValidExpressions() : "true";
            Band maskBand = product.addBand("_RA_MASK", expression, ProductData.TYPE_UINT8);
            maskImage = maskBand.getSourceImage();
            boolean tEqualTileGrid = true;

            RAConfig.BandConfig[] bandConfigs = raConfig.getBandConfigs();
            dataImages = new PlanarImage[bandConfigs.length];
            for (int i = 0; i < bandConfigs.length; i++) {
                String bandName = bandConfigs[i].getName().trim();
                Band band = product.getBand(bandName);
                if (band == null) {
                    throw new IllegalArgumentException("Product does not contain band " + bandName);
                }
                if (band.getRasterHeight() != maskBand.getRasterHeight() ||
                        band.getRasterWidth() != maskBand.getRasterWidth()) {
                    // TODO
                    throw new IllegalArgumentException("Multi-size not supported !!!");
                }
                dataImages[i] = band.getGeophysicalImage();
                if (!hasSameTiling(maskImage, dataImages[i])) {
                    tEqualTileGrid = false;
                }
            }
            this.equalTileGrids = tEqualTileGrid;
        }

        private boolean hasSameTiling(PlanarImage im1, PlanarImage im2) {
            return im1.getTileWidth() == im2.getTileWidth() &&
                    im1.getTileHeight() == im2.getTileHeight() &&
                    im1.getTileGridXOffset() == im2.getTileGridXOffset() &&
                    im1.getTileGridYOffset() == im2.getTileGridYOffset();
        }

        public Extract performExtraction(String regionName, Geometry geometry, ProgressMonitor pm) {
            Rectangle rect = SubsetOp.computePixelRegion(product, geometry, 1);
            if (rect.isEmpty()) {
                LOG.info("Nothing to extract for region " + regionName);
            }
            PreparedGeometry preparedGeometry = PreparedGeometryFactory.prepare(geometry);
            GeometryFilter geometryFilter = new GeometryFilter(product.getSceneGeoCoding(), preparedGeometry);

            int numBands = dataImages.length;
            int numPixelsMax = rect.width * rect.height;
            String fmt = "%s bounds: [x=%d,y=%d,width=%d,height=%d]  area: %d pixel";
            LOG.info(String.format(fmt, regionName, rect.x, rect.y, rect.width, rect.height, numPixelsMax));

            Extract extract = new Extract(numBands, numPixelsMax);
            Point[] tileIndices = maskImage.getTileIndices(rect);
            LOG.info(String.format("Extracting data from %d tiles", tileIndices.length));
            pm.beginTask("extraction", tileIndices.length);
            for (Point maskTileIndex : tileIndices) {
                handleSingleTile(maskTileIndex, rect, geometryFilter, extract);
                pm.worked(1);
            }
            LOG.info(String.format("Visited %d pixels          ", extract.numPixel));
            LOG.info(String.format("Extracted %d valid samples ", extract.numExtracts));
            pm.done();
            if (extract.numExtracts > 0) {
                // truncate array to used size
                for (int i = 0; i < extract.samples.length; i++) {
                    float[] samples = extract.samples[i];
                    extract.samples[i] = Arrays.copyOf(samples, extract.numExtracts);
                }
                return extract;
            } else {
                return null;
            }
        }

        private RasterStack getRasters(Point maskTileIndex, Rectangle regionRect) {
            Rectangle tileRect = maskImage.getTileRect(maskTileIndex.x, maskTileIndex.y);
            Rectangle rect = tileRect.intersection(regionRect);

            Raster maskTile = maskImage.getTile(maskTileIndex.x, maskTileIndex.y);
            Raster[] dataTiles = new Raster[dataImages.length];

            if (equalTileGrids) {
                for (int i = 0; i < dataImages.length; i++) {
                    dataTiles[i] = dataImages[i].getTile(maskTileIndex.x, maskTileIndex.y);
                }
            } else {
                for (int i = 0; i < dataImages.length; i++) {
                    dataTiles[i] = dataImages[i].getData(tileRect);
                }
            }
            return new RasterStack(rect, maskTile, dataTiles);
        }

        private void handleSingleTile(Point maskTileIndex, Rectangle regionRect, GeometryFilter geometryFilter, Extract extract) {
            final RasterStack rasterStack = getRasters(maskTileIndex, regionRect);
            final Rectangle rect = rasterStack.rect;

            for (int y = rect.y; y < rect.y + rect.height; y++) {
                for (int x = rect.x; x < rect.x + rect.width; x++) {
                    if (geometryFilter.test(x, y)) {
                        extract.numPixel++;
                        // test valid mask
                        if (rasterStack.maskTile.getSample(x, y, 0) != 0) {
                            // extract pixel
                            boolean oneSampleGood = false;
                            for (int i = 0; i < rasterStack.dataTiles.length; i++) {
                                float sample = rasterStack.dataTiles[i].getSampleFloat(x, y, 0);
                                extract.samples[i][extract.numExtracts] = sample;
                                if (!Float.isNaN(sample)) {
                                    oneSampleGood = true;
                                }
                            }
                            if (oneSampleGood) {
                                extract.numExtracts++;
                                if (extract.time == -1 ) {
                                    ProductData.UTC pixelScanTime = ProductUtils.getPixelScanTime(product, x + 0.5, y + 0.5);
                                    extract.time = pixelScanTime.getAsDate().getTime();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    static class Extract {

        final float[][] samples;
        int numPixel;
        int numExtracts;
        long time;

        public Extract(int numBands, int numPixelsMax) {
            samples = new float[numBands][numPixelsMax];
            numPixel = 0;
            numExtracts = 0;
            time = -1;
        }
    }

    static class RasterStack {

        private final Rectangle rect;
        private final Raster maskTile;
        private final Raster[] dataTiles;

        RasterStack(Rectangle rect, Raster maskTile, Raster[] dataTiles) {
            this.rect = rect;
            this.maskTile = maskTile;
            this.dataTiles = dataTiles;
        }
    }

    static class GeometryFilter {
        private final GeoCoding geoCoding;
        private final PreparedGeometry geometry;
        private final GeometryFactory geometryFactory;

        GeometryFilter(GeoCoding geoCoding, PreparedGeometry geometry) {
            this.geoCoding = geoCoding;
            this.geometry = geometry;
            this.geometryFactory = new GeometryFactory();
        }

        boolean test(int x, int y) {
            final PixelPos pixelPos = new PixelPos(x + 0.5, y + 0.5);
            final GeoPos geoPos = geoCoding.getGeoPos(pixelPos, null);
            Coordinate coordinate = new Coordinate(geoPos.lon, geoPos.lat);
            return geometry.contains(geometryFactory.createPoint(coordinate));
        }
    }


}
