package com.bc.calvalus.reporting;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.*;

/**
 * @author hans
 */
public class PriceCalculatorTest {

    @Test
    public void canGetCpuPrice() throws Exception {
        assertThat(PriceCalculator.getCpuPrice(3600000L), equalTo(1.32));
    }

    @Test
    public void canGetMemoryPrice() throws Exception {
        assertThat(PriceCalculator.getMemoryPrice(36864000000L), equalTo(2.23));
    }

    @Test
    public void canGetDiskPrice() throws Exception {
        assertThat(PriceCalculator.getDiskPrice(1024000L), equalTo(11.2));
    }
}