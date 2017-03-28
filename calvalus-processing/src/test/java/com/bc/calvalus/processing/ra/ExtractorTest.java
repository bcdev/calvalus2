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

import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        config.setValidExpressions("LAT > 50");

        RAMapper.Extractor extractor = new RAMapper.Extractor(product, config);
        Geometry northSea = GeometryUtils.createGeometry(NORTH_SEA_WKT);
        RAMapper.Extract extract = extractor.performExtraction("r1", northSea, ProgressMonitor.NULL);
        assertNotNull(extract);
        assertEquals(574, extract.numObs);
        assertEquals(3, extract.samples.length);
        assertEquals(373, extract.samples[0].length);
        ProductData.UTC actualUTC = ProductData.UTC.create(new Date(extract.time), 0);
        assertEquals("01-JAN-2011 10:37:35.000000", actualUTC.format());
    }
}