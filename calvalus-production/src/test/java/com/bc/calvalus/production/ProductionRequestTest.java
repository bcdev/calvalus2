package com.bc.calvalus.production;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProductionRequestTest {

    @Test
    public void testConstructors() {
        ProductionRequest req = new ProductionRequest("typeA");
        assertEquals("typeA", req.getProductionType());
        assertNotNull(req.getProductionParameters());
        assertEquals(0, req.getProductionParameters().length);

        req = new ProductionRequest("typeB", new ProductionParameter("a", "3"), new ProductionParameter("b", "8"));
        assertEquals("typeB", req.getProductionType());
        assertNotNull(req.getProductionParameters());
        assertEquals(2, req.getProductionParameters().length);

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
            new ProductionRequest("t2", null);
            fail("Production parameter must not be null");
        } catch (NullPointerException e) {
            // ok
        }
    }



}
