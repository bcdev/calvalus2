package com.bc.calvalus.processing.fire.format.grid.s2;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static junit.framework.TestCase.assertEquals;

public class GeoLutMapperTest {

    @Test
    public void testExtract() throws Exception {

        Files.list(Paths.get("d:\\workspace\\fire-cci\\s2-tiles"))
                .filter(p -> p.getFileName().toString().endsWith("nc"))
                .forEach(p -> {
                    try {
                        String utmTile = p.getFileName().toString().split("-")[1].replace("T", "");
                        GeoLutMapper.extract(
                                p.toFile(),
                                utmTile,
                                "x228y78");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test
    public void testGetLat() throws Exception {
        assertEquals(-18.0, GeoLutMapper.getTopLat(72, 0));
        assertEquals(-18.25, GeoLutMapper.getBottomLat(72, 0));

        assertEquals(-19.75, GeoLutMapper.getTopLat(72, 7));
        assertEquals(-20.0, GeoLutMapper.getBottomLat(72, 7));
    }

    @Test
    public void testGetLon() throws Exception {
        assertEquals(38.0, GeoLutMapper.getLeftLon(218, 0));
        assertEquals(38.25, GeoLutMapper.getRightLon(218, 0));

        assertEquals(39.75, GeoLutMapper.getLeftLon(218, 7));
        assertEquals(40.0, GeoLutMapper.getRightLon(218, 7));
    }

}