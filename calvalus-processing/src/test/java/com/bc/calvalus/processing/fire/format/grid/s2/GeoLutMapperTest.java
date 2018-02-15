package com.bc.calvalus.processing.fire.format.grid.s2;

import org.junit.Test;

import java.io.File;

public class GeoLutMapperTest {

    @Test
    public void testExtract() throws Exception {

        boolean foundPixel = GeoLutMapper.extract(
                () -> {
                },
                new File("d:\\workspace\\fire-cci\\temp\\BA-T35NPD-20151128T084811.nc"),
                "35NPD",
                "v43h104");
    }

}