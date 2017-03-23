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
        int numRegions = raConfig.getRegions().length;
        pm.beginTask("Region Analysis", numRegions * 2);
        try {
            ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, numRegions));
            if (product != null) {
                Extractor extractor = new Extractor(product, raConfig);
                Iterator<NamedRegion> regionIterator = raConfig.createNamedRegionIterator(context.getConfiguration());
                int regionId = 0;
                while (regionIterator.hasNext()) {
                    NamedRegion namedRegion = regionIterator.next();
                    ProgressMonitor subPM = SubProgressMonitor.create(pm, 1);
                    Extract extract = extractor.performExtraction(namedRegion.name, namedRegion.geometry, subPM);
                    if (extract != null) {
                        RAKey key = new RAKey(regionId, extract.time);
                        RAValue value = new RAValue(extract.numPixel, extract.samples, extract.time, product.getName());
                        context.write(key, value);
                    }
                    regionId++;
                }
            } else {
                LOG.warning("product does not cover region, skipping processing.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
            }
        } finally {
            pm.done();
        }
    }

    static class Extract {

        final float[][] samples;
        int numPixel;
        long time;

        public Extract(int numBands, int numPixelsMax) {
            samples = new float[numBands][numPixelsMax];
            numPixel = 0;
            time = -1;
        }
    }

    static class Extractor {

        private final Product product;
        private final PlanarImage maskImage;
        private final PlanarImage[] dataImages;
        private final RAConfig raConfig;

        private Rectangle regionRect;
        private GeometryFilter geometryFilter;
        private int extractedSamples;

        public Extractor(Product product, RAConfig raConfig) {
            this.product = product;
            this.raConfig = raConfig;

            if (product.getSceneTimeCoding() == null && product.getStartTime() == null && product.getEndTime() == null) {
                throw new IllegalArgumentException("Product has no time information");
            }

            String expression = StringUtils.isNotNullAndNotEmpty(raConfig.getValidExpressions()) ? raConfig.getValidExpressions() : "true";
            Band maskBand = product.addBand("_RA_MASK", expression, ProductData.TYPE_UINT8);
            maskImage = maskBand.getSourceImage();

            String[] bandNames = raConfig.getBandNames();
            dataImages = new PlanarImage[bandNames.length];
            for (int i = 0; i < bandNames.length; i++) {
                String bandName = bandNames[i].trim();
                Band band = product.getBand(bandName);
                if (band == null) {
                    throw new IllegalArgumentException("Product does not contain band " + bandName);
                }
                dataImages[i] = band.getGeophysicalImage();
            }
        }

        public Extract performExtraction(String name, Geometry geometry, ProgressMonitor pm) {
            regionRect = SubsetOp.computePixelRegion(product, geometry, 1);
            if (regionRect.isEmpty()) {
                LOG.info("Nothing to extract for region " + name);
            }
            PreparedGeometry preparedGeometry = PreparedGeometryFactory.prepare(geometry);
            geometryFilter = new GeometryFilter(product.getSceneGeoCoding(), preparedGeometry);

            int numBands = dataImages.length;
            int numPixelsMax = regionRect.width * regionRect.height;
            LOG.info("numPixelsMax = " + numPixelsMax);

            extractedSamples = 0;
            Extract extract = new Extract(numBands, numPixelsMax);
            Point[] tileIndices = maskImage.getTileIndices(regionRect);
            LOG.info(String.format("Extracting data from %d tiles", tileIndices.length));
            pm.beginTask("extraction", tileIndices.length);
            for (Point tileIndex : tileIndices) {
                handleSingleTile(tileIndex, extract);
                pm.worked(1);
            }
            LOG.info(String.format("Extracted %d valid samples", extractedSamples));
            pm.done();
            if (extractedSamples > 0) {
                for (int i = 0; i < extract.samples.length; i++) {
                    float[] samples = extract.samples[i];
                    extract.samples[i] = Arrays.copyOf(samples, extractedSamples);
                }
                return extract;
            } else {
                return null;
            }
        }

        private void handleSingleTile(Point tileIndex, Extract extract) {
            Rectangle tileRect = maskImage.getTileRect(tileIndex.x, tileIndex.y);
            Rectangle rect = tileRect.intersection(regionRect);

            Raster maskTile = maskImage.getTile(tileIndex.x, tileIndex.y);
            Raster[] dataTiles = new Raster[dataImages.length];
            for (int i = 0; i < dataImages.length; i++) {
                dataTiles[i] = dataImages[i].getTile(tileIndex.x, tileIndex.y);
            }


            for (int y = rect.y; y < rect.y + rect.height; y++) {
                for (int x = rect.x; x < rect.x + rect.width; x++) {
                    if (geometryFilter.test(x, y)) {
                        extract.numPixel++;
                        // test valid mask
                        if (maskTile.getSample(x, y, 0) != 0) {
                            // extract pixel
                            boolean oneSampleGood = false;
                            for (int i = 0; i < dataTiles.length; i++) {
                                float sample = dataTiles[i].getSampleFloat(x, y, 0);
                                extract.samples[i][extractedSamples] = sample;
                                if (!Float.isNaN(sample)) {
                                    oneSampleGood = true;
                                }
                            }
                            if (oneSampleGood) {
                                extractedSamples++;
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

    static class GeometryFilter {
        private final GeoCoding geoCoding;
        private final PreparedGeometry geometry;
        private final GeometryFactory geometryFactory;

        public GeometryFilter(GeoCoding geoCoding, PreparedGeometry geometry) {
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
