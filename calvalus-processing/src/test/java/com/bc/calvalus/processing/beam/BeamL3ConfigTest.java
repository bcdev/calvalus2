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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.binning.AggregatorAverage;
import com.bc.calvalus.binning.AggregatorAverageML;
import com.bc.calvalus.binning.AggregatorMinMax;
import com.bc.calvalus.binning.AggregatorOnMaxSet;
import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.IsinBinningGrid;
import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.processing.shellexec.XmlDoc;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.io.FileUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;

import static org.junit.Assert.*;

public class BeamL3ConfigTest {

    private BeamL3Config l3Config;

    @Before
    public void createL3Config() throws IOException, SAXException, ParserConfigurationException {
        l3Config = loadConfig("l3-request.xml");
    }

    @Test
    public void testBinningGrid() {
        BinningGrid grid = new BeamL3Config().getBinningGrid();
        assertEquals(2160, grid.getNumRows());
        assertEquals(IsinBinningGrid.class, grid.getClass());

        grid = l3Config.getBinningGrid();
        assertEquals(4320, grid.getNumRows());
        assertEquals(IsinBinningGrid.class, grid.getClass());

    }

    @Test
    public void testGetRegionOfInterest() {
        BeamL3Config l3Config = new BeamL3Config();
        Geometry regionOfInterest;

        regionOfInterest = l3Config.getRegionOfInterest();
        assertNull(regionOfInterest);

        l3Config.bbox = "-60.0, 13.4, -20.0, 23.4";
        regionOfInterest = l3Config.getRegionOfInterest();
        assertTrue(regionOfInterest instanceof Polygon);
        assertEquals("POLYGON ((-60 13.4, -20 13.4, -20 23.4, -60 23.4, -60 13.4))", regionOfInterest.toString());

        l3Config.regionWkt = "POINT(-60.0 13.4)";
        regionOfInterest = l3Config.getRegionOfInterest();
        assertTrue(regionOfInterest instanceof Point);
        assertEquals("POINT (-60 13.4)", regionOfInterest.toString());

        l3Config.regionWkt = "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))";
        regionOfInterest = l3Config.getRegionOfInterest();
        assertTrue(regionOfInterest instanceof Polygon);
        assertEquals("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))", regionOfInterest.toString());

        l3Config.regionWkt = null;
        l3Config.bbox = null;
        regionOfInterest = l3Config.getRegionOfInterest();
        assertNull(regionOfInterest);
    }

    @Test
    public void testComputationOfProductGeometriesAndPixelRegions() throws FactoryException, TransformException {
        Geometry geometry;

        Product product = new Product("N", "T", 360, 180);
        AffineTransform at = AffineTransform.getTranslateInstance(-180, -90);
        CrsGeoCoding geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84, new Rectangle(360, 180), at);
        product.setGeoCoding(geoCoding);
        geometry = BeamL3Config.computeProductGeometry(product);
        assertTrue(geometry instanceof Polygon);
        assertEquals("POLYGON ((-179.5 -89.5, -179.5 89.5, 179.5 89.5, 179.5 -89.5, -179.5 -89.5))", geometry.toString());

        Rectangle rectangle;

        rectangle = BeamL3Config.computePixelRegion(product, geometry);
        assertEquals(new Rectangle(360, 180), rectangle);

        SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(new Rectangle(180 - 50, 90 - 25, 100, 50));
        product = op.getTargetProduct();
        geometry = BeamL3Config.computeProductGeometry(product);
        assertTrue(geometry instanceof Polygon);
        assertEquals("POLYGON ((-49.5 -24.5, -49.5 24.5, 49.5 24.5, 49.5 -24.5, -49.5 -24.5))", geometry.toString());

        // BBOX fully contained, with border=0
        rectangle = BeamL3Config.computePixelRegion(product, createBBOX(0.0, 0.0, 10.0, 10.0), 0);
        assertEquals(new Rectangle(50, 25, 11, 11), rectangle);

        // BBOX fully contained, with border=1
        rectangle = BeamL3Config.computePixelRegion(product, createBBOX(0.0, 0.0, 10.0, 10.0), 1);
        assertEquals(new Rectangle(49, 24, 13, 13), rectangle);

        // BBOX intersects product rect in upper left
        rectangle = BeamL3Config.computePixelRegion(product, createBBOX(45.5, 20.5, 100.0, 50.0), 0);
        assertEquals(new Rectangle(95, 45, 5, 5), rectangle);

        // Product bounds fully contained in BBOX
        rectangle = BeamL3Config.computePixelRegion(product, createBBOX(-180, -90, 360, 180), 0);
        assertEquals(new Rectangle(0, 0, 100, 50), rectangle);

        // BBOX not contained
        rectangle = BeamL3Config.computePixelRegion(product, createBBOX(60.0, 0.0, 10.0, 10.0));
        assertEquals(null, rectangle);
    }

    private Polygon createBBOX(double x, double y, double w, double h) {
        GeometryFactory factory = new GeometryFactory();
        final LinearRing ring = factory.createLinearRing(new Coordinate[]{
                new Coordinate(x, y),
                new Coordinate(x + w, y),
                new Coordinate(x + w, y + h),
                new Coordinate(x, y + h),
                new Coordinate(x, y)
        });
        return factory.createPolygon(ring, null);
    }

//    @Test
//    public void testStartEndTime() throws ParseException {
//        Properties properties = new Properties();
//        com.bc.calvalus.binning.job.BeamL3Config l3Config = new com.bc.calvalus.binning.job.BeamL3Config(properties);
//        properties.setProperty(CONFNAME_L3_START_DATE, "2008-06-01");
//        ProductData.UTC startTime = l3Config.getStartTime();
//        assertNotNull(startTime);
//        Calendar expectedStart = ProductData.UTC.parse("01-JUN-2008 00:00:00").getAsCalendar();
//        assertEqualsCalendar(expectedStart, startTime.getAsCalendar());
//
//        ProductData.UTC endTime = l3Config.getEndTime();
//        assertNotNull(endTime);
//        Calendar expectedEnd = ProductData.UTC.parse("16-JUN-2008 00:00:00").getAsCalendar();
//        assertEqualsCalendar(expectedEnd, endTime.getAsCalendar());
//    }

    private static void assertEqualsCalendar(Calendar expected, Calendar actual) {
        assertEquals(expected.get(Calendar.YEAR), actual.get(Calendar.YEAR));
        assertEquals(expected.get(Calendar.MONTH), actual.get(Calendar.MONTH));
        assertEquals(expected.get(Calendar.DAY_OF_MONTH), actual.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    public void testVariableContext() {
        VariableContext varCtx = l3Config.getVariableContext();

        assertEquals(8, varCtx.getVariableCount());

        assertEquals(0, varCtx.getVariableIndex("ndvi"));
        assertEquals(1, varCtx.getVariableIndex("tsm"));
        assertEquals(2, varCtx.getVariableIndex("algal1"));
        assertEquals(3, varCtx.getVariableIndex("algal2"));
        assertEquals(4, varCtx.getVariableIndex("chl"));
        assertEquals(5, varCtx.getVariableIndex("reflec_3"));
        assertEquals(6, varCtx.getVariableIndex("reflec_7"));
        assertEquals(7, varCtx.getVariableIndex("reflec_8"));
        assertEquals(-1, varCtx.getVariableIndex("reflec_6"));
        assertEquals(-1, varCtx.getVariableIndex("reflec_10"));

        assertEquals("!l2_flags.INVALID && l2_flags.WATER", varCtx.getMaskExpr());

        assertEquals("ndvi", varCtx.getVariableName(0));
        assertEquals("(reflec_10 - reflec_6) / (reflec_10 + reflec_6)", varCtx.getVariableExpr(0));

        assertEquals("algal2", varCtx.getVariableName(3));
        assertEquals(null, varCtx.getVariableExpr(3));

        assertEquals("reflec_7", varCtx.getVariableName(6));
        assertEquals(null, varCtx.getVariableExpr(6));

    }

    @Test
    public void testBinManager() {
        BinManager binManager = l3Config.getBinningContext().getBinManager();
        assertEquals(6, binManager.getAggregatorCount());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(0).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(1).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(2).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(3).getClass());
        assertEquals(AggregatorOnMaxSet.class, binManager.getAggregator(4).getClass());
        assertEquals(AggregatorMinMax.class, binManager.getAggregator(5).getClass());
    }

        @Test
    public void testNumRows() {
            assertEquals(4320, l3Config.numRows);
        }

    private BeamL3Config loadConfig(String configPath) throws IOException, SAXException, ParserConfigurationException {
        return BeamL3Config.create(new XmlDoc(loadConfigProperties(configPath)));
    }

    private String loadConfigProperties(String configPath) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(configPath));
        return FileUtils.readText(inputStreamReader).trim();
    }

}
