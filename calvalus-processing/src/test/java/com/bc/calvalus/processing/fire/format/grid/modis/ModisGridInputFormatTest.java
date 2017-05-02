package com.bc.calvalus.processing.fire.format.grid.modis;

import org.junit.Test;

import java.io.File;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertNotNull;

public class ModisGridInputFormatTest {

    @Test
    public void testGetTilesLut() throws Exception {
        GeoLutCreator.Result tilesLut = ModisGridInputFormat.getTilesLut(new File("C:\\ssd\\modis-geo-luts\\modis-tiles-lut.txt"));
        assertNotNull(tilesLut);
        for (String key : tilesLut.keySet()) {
            SortedSet<String> inputTiles = new TreeSet<>(tilesLut.get(key));
            for (String inputTile : inputTiles) {
                System.out.println(inputTile);
            }
        }
    }
}