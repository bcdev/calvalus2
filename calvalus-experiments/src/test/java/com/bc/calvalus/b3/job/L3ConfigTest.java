package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.AggregatorAverageML;
import com.bc.calvalus.b3.AggregatorMinMax;
import com.bc.calvalus.b3.AggregatorOnMaxSet;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.VariableContext;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.bc.calvalus.b3.job.L3Config.*;
import static org.junit.Assert.*;

public class L3ConfigTest {
    private L3Config l3Config;

    @Before
    public void createL3Config() {
        l3Config = loadConfig("job.properties");
    }

    @Test
    public void testBinningGrid() {
        BinningGrid grid = l3Config.getBinningGrid();
        assertEquals(4320, grid.getNumRows());
        assertEquals(IsinBinningGrid.class, grid.getClass());
    }

    @Test
    public void testGetRegionOfInterest() {
        Properties properties = new Properties();
        L3Config l3Config = new L3Config(properties);
        Geometry regionOfInterest;

        regionOfInterest = l3Config.getRegionOfInterest();
        assertNull(regionOfInterest);

        properties.setProperty(CONFNAME_L3_BBOX, "-60.0, 13.4, -20.0, 23.4");
        regionOfInterest = l3Config.getRegionOfInterest();
        assertTrue(regionOfInterest instanceof Polygon);
        assertEquals("POLYGON ((-60 13.4, -20 13.4, -20 23.4, -60 23.4, -60 13.4))", regionOfInterest.toString());

        properties.setProperty(CONFNAME_L3_REGION, "POINT(-60.0 13.4)");
        regionOfInterest = l3Config.getRegionOfInterest();
        assertTrue(regionOfInterest instanceof Point);
        assertEquals("POINT (-60 13.4)", regionOfInterest.toString());

        properties.setProperty(CONFNAME_L3_REGION, "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))");
        regionOfInterest = l3Config.getRegionOfInterest();
        assertTrue(regionOfInterest instanceof Polygon);
        assertEquals("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))", regionOfInterest.toString());

        properties.remove(CONFNAME_L3_REGION);
        properties.remove(CONFNAME_L3_BBOX);
        regionOfInterest = l3Config.getRegionOfInterest();
        assertNull(regionOfInterest);
    }

    @Test
    public void testComputationOfProductGeometriesAndPixelRegions() throws TransformException, FactoryException {
        Geometry geometry;

        Product product = new Product("N", "T", 360, 180);
        AffineTransform at = AffineTransform.getTranslateInstance(-180, -90);
        CrsGeoCoding geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84, new Rectangle(360, 180), at);
        product.setGeoCoding(geoCoding);
        geometry = L3Config.computeProductGeometry(product);
        assertTrue(geometry instanceof Polygon);
        assertEquals("POLYGON ((-179.5 -89.5, -179.5 89.5, 179.5 89.5, 179.5 -89.5, -179.5 -89.5))", geometry.toString());

        Rectangle rectangle;

        rectangle = L3Config.computePixelRegion(product, geometry);
        assertEquals(new Rectangle(360, 180), rectangle);

        SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(new Rectangle(180 - 50, 90 - 25, 100, 50));
        product = op.getTargetProduct();
        geometry = L3Config.computeProductGeometry(product);
        assertTrue(geometry instanceof Polygon);
        assertEquals("POLYGON ((-49.5 -24.5, -49.5 24.5, 49.5 24.5, 49.5 -24.5, -49.5 -24.5))", geometry.toString());

        // BBOX fully contained, with border=0
        rectangle = L3Config.computePixelRegion(product, createBBOX(0.0, 0.0, 10.0, 10.0), 0);
        assertEquals(new Rectangle(50, 25, 11, 11), rectangle);

        // BBOX fully contained, with border=1
        rectangle = L3Config.computePixelRegion(product, createBBOX(0.0, 0.0, 10.0, 10.0), 1);
        assertEquals(new Rectangle(49, 24, 13, 13), rectangle);

        // BBOX intersects product rect in upper left
        rectangle = L3Config.computePixelRegion(product, createBBOX(45.5, 20.5, 100.0, 50.0), 0);
        assertEquals(new Rectangle(95, 45, 5, 5), rectangle);

        // Product bounds fully contained in BBOX
        rectangle = L3Config.computePixelRegion(product, createBBOX(-180, -90, 360, 180), 0);
        assertEquals(new Rectangle(0, 0, 100, 50), rectangle);

        // BBOX not contained
        rectangle = L3Config.computePixelRegion(product, createBBOX(60.0, 0.0, 10.0, 10.0));
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

    @Test
    public void testInputPath() {
        Properties properties = new Properties();
        L3Config l3Config = new L3Config(properties);

        String[] inputPath = l3Config.getInputPath();
        assertNull(inputPath);

        properties.setProperty(CONFNAME_L3_START_DATE, "2008-06-01");
        inputPath = l3Config.getInputPath();
        assertNotNull(inputPath);
        assertEquals(16, inputPath.length);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/01", inputPath[0]);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/02", inputPath[1]);

        properties.setProperty(CONFNAME_L3_NUM_DAYS, "1");
        inputPath = l3Config.getInputPath();
        assertNotNull(inputPath);
        assertEquals(1, inputPath.length);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/01", inputPath[0]);

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
        BinManager binManager = l3Config.getBinManager();
        assertEquals(6, binManager.getAggregatorCount());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(0).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(1).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(2).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(3).getClass());
        assertEquals(AggregatorOnMaxSet.class, binManager.getAggregator(4).getClass());
        assertEquals(AggregatorMinMax.class, binManager.getAggregator(5).getClass());
    }

    private L3Config loadConfig(String configPath) {
        return new L3Config(loadConfigProperties(configPath));
    }

    private Properties loadConfigProperties(String configPath) {
        final InputStream is = getClass().getResourceAsStream(configPath);
        try {
            try {
                final Properties properties = new Properties();
                properties.load(is);
                return properties;
            } finally {
                is.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
