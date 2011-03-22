package com.bc.calvalus.production;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class ProductionRequestTest {

    @Test
    public void testConstructors() {
        ProductionRequest req = new ProductionRequest("typeA", "ewa");
        assertEquals("typeA", req.getProductionType());
        assertEquals("ewa", req.getUserName());
        assertNotNull(req.getProductionParameters());
        assertEquals(0, req.getProductionParameters().size());

        req = new ProductionRequest("typeB", "ewa",
                                    "a", "3", "b", "8");
        assertEquals("typeB", req.getProductionType());
        assertEquals("ewa", req.getUserName());
        assertNotNull(req.getProductionParameters());
        assertEquals(2, req.getProductionParameters().size());

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
    public void testGetRoiGeometry() throws ProductionException {
        ProductionRequest req = new ProductionRequest("typeA", "ewa");
        Geometry regionOfInterest;

        try {
            req.getRoiGeometry();
            fail("No production parameters for geometry present");
        } catch (ProductionException e) {
            // ok
        }

        //-60.0, 13.4, -20.0, 23.4
        req = new ProductionRequest("typeA", "ewa",
                                    "lonMin", "-60.0",
                                    "lonMax", "-20.0",
                                    "latMin", "13.4",
                                    "latMax", "23.4");
        regionOfInterest = req.getRoiGeometry();
        assertTrue(regionOfInterest instanceof Polygon);
        assertEquals("POLYGON ((-60 13.4, -20 13.4, -20 23.4, -60 23.4, -60 13.4))", regionOfInterest.toString());

        req = new ProductionRequest("typeA", "ewa",
                                    "regionWKT", "POINT(-60.0 13.4)");
        regionOfInterest = req.getRoiGeometry();
        assertTrue(regionOfInterest instanceof Point);
        assertEquals("POINT (-60 13.4)", regionOfInterest.toString());

        req = new ProductionRequest("typeA", "ewa",
                                    "regionWKT", "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))");
        regionOfInterest = req.getRoiGeometry();
        assertTrue(regionOfInterest instanceof Polygon);
        assertEquals("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))", regionOfInterest.toString());
    }


}
