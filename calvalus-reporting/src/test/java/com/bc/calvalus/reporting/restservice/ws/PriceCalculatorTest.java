package com.bc.calvalus.reporting.restservice.ws;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author hans
 */
public class PriceCalculatorTest {

    @Test
    public void canGetCpuPrice() throws Exception {
        assertEquals(1.32, PriceCalculator.getCpuPrice(3600000L), 1.0e-2);
    }

    @Test
    public void canGetMemoryPrice() throws Exception {
        assertEquals(2.23, PriceCalculator.getMemoryPrice(36864000000L), 1.0e-2);
    }

    @Test
    public void canGetDiskPrice() throws Exception {
        assertEquals(11.2, PriceCalculator.getDiskPrice(1024000L), 1.0e-2);
    }
}