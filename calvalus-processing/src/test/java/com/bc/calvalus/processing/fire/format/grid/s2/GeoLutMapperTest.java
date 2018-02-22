package com.bc.calvalus.processing.fire.format.grid.s2;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class GeoLutMapperTest {

    @Test
    public void testExtract() throws Exception {

        boolean foundPixel = GeoLutMapper.extract(
                () -> {
                },
                new File("d:\\workspace\\fire-cci\\temp\\BA-T27PZQ-20150628T114417.nc"),
                "27PZQ",
                "x81y38");
        assertTrue(foundPixel);
    }

}