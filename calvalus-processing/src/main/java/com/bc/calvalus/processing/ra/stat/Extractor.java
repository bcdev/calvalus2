/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de) 
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

package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.ra.RAConfig;
import com.bc.calvalus.processing.ra.RARegions;
import com.bc.ceres.core.ProgressMonitor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.core.util.StringUtils;

import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.Raster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Performs the extraction from a EO data products
 */
public abstract class Extractor {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final Product product;
    private final PlanarImage maskImage;
    private final PlanarImage[] dataImages;
    private final boolean equalTileGrids;
    private final List<GeometryFilter> regionFilters;
    private long time;

    public Extractor(Product product, String validExpression, String[] bandNames, RARegions.RegionIterator regionIterator) {
        this.product = product;

        if (product.getSceneTimeCoding() == null && product.getStartTime() == null && product.getEndTime() == null) {
            throw new IllegalArgumentException("Product has no time information");
        }
        if (bandNames == null) {
            throw new NullPointerException("bandNames");
        }
        if (bandNames.length == 0) {
            throw new IllegalArgumentException("No bandNames given");
        }
        String expression = StringUtils.isNotNullAndNotEmpty(validExpression) ? validExpression : "true";
        Band maskBand = product.addBand("_RA_MASK", expression, ProductData.TYPE_UINT8);
        maskImage = maskBand.getSourceImage();
        boolean tEqualTileGrid = true;

        dataImages = new PlanarImage[bandNames.length];
        for (int i = 0; i < bandNames.length; i++) {
            Band band = product.getBand(bandNames[i]);
            if (band == null) {
                throw new IllegalArgumentException("Product does not contain band " + bandNames[i]);
            }
            if (band.getRasterHeight() != maskBand.getRasterHeight() ||
                    band.getRasterWidth() != maskBand.getRasterWidth()) {
                // TODO
                throw new IllegalArgumentException("Multi-size not supported !!!");
            }
            dataImages[i] = ImageManager.createMaskedGeophysicalImage(band, Float.NaN);
            if (!hasSameTiling(maskImage, dataImages[i])) {
                tEqualTileGrid = false;
            }
        }
        this.equalTileGrids = tEqualTileGrid;
        LOG.info("MaskImage and DataImages have same tile grid: " + equalTileGrids);

        int regionIndex = 0;
        regionFilters = new ArrayList<>();
        while (regionIterator.hasNext()) {
            RAConfig.NamedRegion namedRegion = regionIterator.next();
            try {
                Rectangle pixelRect = SubsetOp.computePixelRegion(product, namedRegion.region, 1);

                if (!pixelRect.isEmpty()) {
                    PreparedGeometry preparedGeometry = PreparedGeometryFactory.prepare(namedRegion.region);
                    regionFilters.add(new Extractor.GeometryFilter(regionIndex,
                            namedRegion.name,
                            pixelRect,
                            product.getSceneGeoCoding(),
                            preparedGeometry));
                }
                regionIndex++;
            }
            catch(TopologyException e) {
                System.err.println("Region is skipped: "+namedRegion.name+" "+namedRegion.region+". Reason:"+e);
            }
        }
        time = -1;
    }

    private static boolean hasSameTiling(PlanarImage im1, PlanarImage im2) {
        return im1.getTileWidth() == im2.getTileWidth() &&
                im1.getTileHeight() == im2.getTileHeight() &&
                im1.getTileGridXOffset() == im2.getTileGridXOffset() &&
                im1.getTileGridYOffset() == im2.getTileGridYOffset();
    }

    public abstract void extractedData(int regionIndex, String regionName, long time, int numObs, float[][] samples) throws IOException, InterruptedException;

    public void extract(ProgressMonitor pm) throws IOException, InterruptedException {
        Point[] tileIndices = maskImage.getTileIndices(null);
        LOG.info(String.format("Start extracting data from %d tiles for %d regions", tileIndices.length, regionFilters.size()));
        pm.beginTask("extraction", tileIndices.length * regionFilters.size());
        for (Point maskTileIndex : tileIndices) {
            final Extractor.RasterStack rasterStack = getRasters(maskTileIndex);
            Rectangle tr = rasterStack.tileRect;
            LOG.info(String.format("Tile [x=%d,y=%d,width=%d,height=%d]", tr.x, tr.y, tr.width, tr.height));
            for (GeometryFilter region : regionFilters) {
                Rectangle rect = rasterStack.tileRect.intersection(region.pixelRect);
                if (!rect.isEmpty()) {
                    LOG.info(String.format("    Region '%s' [x=%d,y=%d,width=%d,height=%d]", region.name, rect.x, rect.y, rect.width, rect.height));
                    Extract extract = handleSingleTile(rasterStack, rect, region);
                    if (extract != null) {
                        LOG.info(String.format("    numObs %8d    numSamples %8d", extract.numObs, extract.numValid));
                        extractedData(region.geoId, region.name, extract.time, extract.numObs, extract.samples);
                    }
                }
                pm.worked(1);
            }
        }
        pm.done();
    }

    private Extractor.Extract handleSingleTile(Extractor.RasterStack rasterStack, Rectangle rect, Extractor.GeometryFilter geometryFilter) {
        int numPixelsMax = rect.width * rect.height;
        Extractor.Extract extract = new Extractor.Extract(dataImages.length, numPixelsMax);
        for (int y = rect.y; y < rect.y + rect.height; y++) {
            for (int x = rect.x; x < rect.x + rect.width; x++) {
                if (geometryFilter.test(x, y)) {
                    extract.numObs++;
                    // test valid mask
                    if (rasterStack.maskTile.getSample(x, y, 0) != 0) {
                        // extract sample
                        boolean oneValueValid = false;
                        for (int i = 0; i < rasterStack.dataTiles.length; i++) {
                            float sample = rasterStack.dataTiles[i].getSampleFloat(x, y, 0);
                            extract.samples[i][extract.numValid] = sample;
                            if (!Float.isNaN(sample)) {
                                oneValueValid = true;
                            }
                        }
                        if (oneValueValid) {
                            extract.numValid++;
                            if (time == -1) {
                                time = getPixelTime(x, y);
                            }
                        }
                    }
                }
            }
        }
        
        if (extract.numValid > 0) {
            // truncate array to used size
            for (int i = 0; i < extract.samples.length; i++) {
                float[] samples = extract.samples[i];
                extract.samples[i] = Arrays.copyOf(samples, extract.numValid);
            }
            extract.time = time;
            return extract;
        } else if (extract.numObs > 0) {
            // no valid samples
            extract.samples = new float[extract.samples.length][0];
            // no Valid samples, but time should be set
            if (time == -1) {
                time = getPixelTime(rect.x, rect.y);
            }
            extract.time = time;
            return extract;
        } else {
            return null;
        }
    }

    private long getPixelTime(int x, int y) {
        return ProductUtils.getPixelScanTime(product, x + 0.5, y + 0.5).getAsDate().getTime();
    }

    private Extractor.RasterStack getRasters(Point maskTileIndex) {
        Rectangle tileRect = maskImage.getTileRect(maskTileIndex.x, maskTileIndex.y);
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
        return new Extractor.RasterStack(tileRect, maskTile, dataTiles);
    }

    static class Extract {

        float[][] samples;
        int numObs;
        int numValid;
        long time;

        Extract(int numBands, int numPixelsMax) {
            samples = new float[numBands][numPixelsMax];
            numObs = 0;
            numValid = 0;
            time = -1;
        }
    }

    static class RasterStack {
        private final Rectangle tileRect;
        private final Raster maskTile;
        private final Raster[] dataTiles;

        RasterStack(Rectangle tileRect, Raster maskTile, Raster[] dataTiles) {
            this.tileRect = tileRect;
            this.maskTile = maskTile;
            this.dataTiles = dataTiles;
        }
    }

    static class GeometryFilter {
        private final int geoId;
        private final String name;
        private final Rectangle pixelRect;
        private final GeoCoding geoCoding;
        private final PreparedGeometry geometry;
        private final GeometryFactory geometryFactory;

        GeometryFilter(int geoId, String name, Rectangle pixelRect, GeoCoding geoCoding, PreparedGeometry geometry) {
            this.geoId = geoId;
            this.name = name;
            this.pixelRect = pixelRect;
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
