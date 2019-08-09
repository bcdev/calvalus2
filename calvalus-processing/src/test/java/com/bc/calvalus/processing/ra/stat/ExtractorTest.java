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

package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.processing.ra.RAConfig;
import com.bc.calvalus.processing.ra.RARegions;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExtractorTest {

    private static final String NORTH_SEA_WKT = "polygon((-19.94 40.00, 0.00 40.00, 0.00 49.22, 12.99 53.99, 13.06 65.00, 0.00 65.00, 0.0 60.00, -20.00 60.00, -19.94 40.00))";

    @Test
    public void testExtract() throws Exception {
        Product product = new Product("t", "d", 360, 180);
        product.setPreferredTileSize(20, 10);
        product.addBand("x", "X");
        product.addBand("l", "LAT");
        product.addBand("c", "3.14");
        GeoCoding geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84, 360, 180, -180.0, 90.0, 1, 1, 0.0, 0.0);
        product.setSceneGeoCoding(geoCoding);

        product.setStartTime(ProductData.UTC.parse("01-JAN-2011 10:20:30"));
        product.setEndTime(ProductData.UTC.parse("01-JAN-2011 12:20:30"));

        RAConfig config = new RAConfig();
        RAConfig.BandConfig x = new RAConfig.BandConfig("x");
        RAConfig.BandConfig l = new RAConfig.BandConfig("l");
        RAConfig.BandConfig c = new RAConfig.BandConfig("c");
        config.setBandConfigs(x, l, c);
        config.setGoodPixelExpression("LAT > 50");
        config.setRegions(new RAConfig.Region("northsea", NORTH_SEA_WKT));

        List<Result> results = new ArrayList<>();
        RARegions.RegionIterator namedRegions = config.createNamedRegionIterator(null);
        Extractor extractor = new Extractor(product, config.getGoodPixelExpression(), config.getBandNames(), namedRegions) {
            @Override
            public void extractedData(int regionIndex, String regionName, long time, int numObs, float[][] samples) throws IOException, InterruptedException {
                results.add(new Result(regionIndex, regionName, time, numObs, samples));
            }
        };
        extractor.extract(ProgressMonitor.NULL);

        assertEquals(5, results.size());
        testResultRecord(results.get(0), 65, 65);
        testResultRecord(results.get(1), 200, 200);
        testResultRecord(results.get(2), 108, 108);
        testResultRecord(results.get(3), 200, 0);
        testResultRecord(results.get(4), 1, 0);
    }

    public void testResultRecord(Result result, int numObs, int numValid) {
        ProductData.UTC actualUTC;
        assertEquals(0, result.regionIndex);
        assertEquals("northsea", result.regionName);
        actualUTC = ProductData.UTC.create(new Date(result.time), 0);
        assertEquals("01-JAN-2011 10:37:35.000000", actualUTC.format());
        assertEquals(numObs, result.numObs);
        assertEquals(3, result.samples.length);
        assertEquals(numValid, result.samples[0].length);
    }

    private static class Result {

        private final int regionIndex;
        private final String regionName;
        private final long time;
        private final int numObs;
        private final float[][] samples;

        private Result(int regionIndex, String regionName, long time, int numObs, float[][] samples) {
            this.regionIndex = regionIndex;
            this.regionName = regionName;
            this.time = time;
            this.numObs = numObs;
            this.samples = samples;
        }
    }
}