package com.bc.calvalus.production;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.*;

public class ProductionRequestTest {

    @Test
    public void testConstructors() {
        ProductionRequest req = new ProductionRequest("typeA", "ewa");
        assertEquals("typeA", req.getProductionType());
        assertEquals("ewa", req.getUserName());
        assertNotNull(req.getParameters());
        assertEquals(0, req.getParameters().size());

        req = new ProductionRequest("typeB", "ewa",
                                    "a", "3", "b", "8");
        assertEquals("typeB", req.getProductionType());
        assertEquals("ewa", req.getUserName());
        assertNotNull(req.getParameters());
        assertEquals(2, req.getParameters().size());

        try {
            new ProductionRequest(null, "ewa");
            fail("Production type must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("", "ewa");
            fail("Production type must not be empty (it is used in the output directory name)");
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            new ProductionRequest("t1", null);
            fail("User name must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t1", "");
            fail("User name must not be empty (it is used in the output directory name)");
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", "ewa",
                                  (Map<String, String>) null);
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", "ewa",
                                  (String) null);
            fail("#production parameters must be a multiple of 2");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", "ewa",
                                  (String) null, (String) "A");
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", "ewa",
                                  (String) "A", (String) null);
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
    }

    @Test
    public void testBasicParameterAccessors() throws ProductionException, ParseException {
        ProductionRequest req = new ProductionRequest("typeA", "ewa",
                                                      "s", "Calvalus rules!",
                                                      "b", "true",
                                                      "i", "4",
                                                      "f", "-2.3",
                                                      "d", "2011-07-13",
                                                      "g", "POINT(15.1 11.6)");

        GeometryFactory gf = new GeometryFactory();
        Date date = ProductionRequest.DATE_FORMAT.parse("2011-07-13");
        Point point = gf.createPoint(new Coordinate(15.1, 11.6));

        assertEquals("Calvalus rules!", req.getString("s"));
        assertEquals(true, req.getBoolean("b"));
        assertEquals(4, req.getInteger("i"));
        assertEquals(-2.3F, req.getFloat("f"), 1e-5F);
        assertEquals(-2.3, req.getDouble("f"), 1e-10);
        assertEquals(date, req.getDate("d"));
        assertTrue(point.equalsExact(req.getGeometry("g")));

        assertEquals("Calvalus sucks?", req.getString("missingParam", "Calvalus sucks?"));
        assertEquals(true, req.getBoolean("missingParam", true));
        assertEquals(234, (int) req.getInteger("missingParam", 234));
        assertEquals(1.9F, req.getFloat("missingParam", 1.9F), 1e-5F);
        assertEquals(1.9, req.getDouble("missingParam", 1.9), 1e-10);
        assertSame(date, req.getDate("missingParam", date));
        assertSame(point, req.getGeometry("missingParam", point));

        assertNull(req.getString("missingParam", null));
        assertNull(req.getBoolean("missingParam", null));
        assertNull(req.getInteger("missingParam", null));
        assertNull(req.getFloat("missingParam", null));
        assertNull(req.getDouble("missingParam", null));
        assertNull(req.getDate("missingParam", null));
        assertNull(req.getGeometry("missingParam", null));

        try {
            req.getString("missingParam");
            fail("ProductionException expected");
        } catch (ProductionException e) {
            // ok
        }

    }


    @Test
    public void testGetRegionGeometry() throws ProductionException {
        ProductionRequest req;
        Geometry regionGeometry;

        try {
            req = new ProductionRequest("typeA", "ewa",
                                    "maxLon", "-20.0",
                                    "maxLat", "23.4");
            req.getRegionGeometry(null);
            fail("Incomplete definition of region geometry");
        } catch (ProductionException e) {
            // ok
        }

        //-60.0, 13.4, -20.0, 23.4
        req = new ProductionRequest("typeA", "ewa",
                                    "minLon", "-60.0",
                                    "maxLon", "-20.0",
                                    "minLat", "13.4",
                                    "maxLat", "23.4");
        regionGeometry = req.getRegionGeometry(null);
        assertTrue(regionGeometry instanceof Polygon);
        assertEquals("POLYGON ((-60 13.4, -20 13.4, -20 23.4, -60 23.4, -60 13.4))", regionGeometry.toString());

        req = new ProductionRequest("typeA", "ewa",
                                    "regionWKT", "POINT(-60.0 13.4)");
        regionGeometry = req.getRegionGeometry(null);
        assertTrue(regionGeometry instanceof Point);
        assertEquals("POINT (-60 13.4)", regionGeometry.toString());

        req = new ProductionRequest("typeA", "ewa",
                                    "regionWKT", "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))");
        regionGeometry = req.getRegionGeometry(null);
        assertTrue(regionGeometry instanceof Polygon);
        assertEquals("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))", regionGeometry.toString());

        req = new ProductionRequest("typeA", "ewa");
        Point defaultGeometry = new GeometryFactory().createPoint(new Coordinate(1, 2));
        regionGeometry = req.getRegionGeometry(defaultGeometry);
        assertSame(defaultGeometry,  regionGeometry);
    }


}
