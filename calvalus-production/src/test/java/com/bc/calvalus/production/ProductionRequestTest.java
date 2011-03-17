package com.bc.calvalus.production;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

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



}
