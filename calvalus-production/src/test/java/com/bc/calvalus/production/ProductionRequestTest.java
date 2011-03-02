package com.bc.calvalus.production;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class ProductionRequestTest {

    @Test
    public void testConstructors() {
        ProductionRequest req = new ProductionRequest("typeA");
        assertEquals("typeA", req.getProductionType());
        assertNotNull(req.getProductionParameters());
        assertEquals(0, req.getProductionParameters().size());

        req = new ProductionRequest("typeB", "a", "3", "b", "8");
        assertEquals("typeB", req.getProductionType());
        assertNotNull(req.getProductionParameters());
        assertEquals(2, req.getProductionParameters().size());

        try {
            new ProductionRequest(null);
            fail("Production type must not be null or empty");
        } catch (NullPointerException e) {
            // ok
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            new ProductionRequest("");
            fail("Production type must not be empty");
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", (Map<String, String>) null);
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", (String) null);
            fail("#production parameters must be a multiple of 2");
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", (String) null, (String) "A");
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", (String) "A", (String) null);
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
    }



}
