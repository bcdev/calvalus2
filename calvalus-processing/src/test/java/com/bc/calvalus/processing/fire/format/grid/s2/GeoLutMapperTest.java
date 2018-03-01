package com.bc.calvalus.processing.fire.format.grid.s2;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

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
                                "x164y98");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

}