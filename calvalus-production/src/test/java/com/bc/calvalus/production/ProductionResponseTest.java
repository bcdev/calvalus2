package com.bc.calvalus.production;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProductionResponseTest {

    @Test
    public void testConstructors() {

        try {
            new ProductionResponse(null);
            fail("Production must not be null");
        } catch (NullPointerException e) {
            // ok
        }
    }



}
