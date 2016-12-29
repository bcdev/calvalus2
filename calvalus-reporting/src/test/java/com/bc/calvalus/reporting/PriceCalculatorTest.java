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
        assertThat(PriceCalculator.getCpuPrice(3503161466L, 0L), equalTo(1.29));
    }

    @Test
    public void canGetMemoryPrice() throws Exception {
        assertThat(PriceCalculator.getMemoryPrice(21523424047104L, 0L), equalTo(1.30));
    }

    @Test
    public void canGetDiskPrice() throws Exception {
        assertThat(PriceCalculator.getDiskPrice(0, 186711804L, 470109601790L, 455803262299L),
                   equalTo(9.64));
    }
}