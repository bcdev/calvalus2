package com.bc.calvalus.processing.fire.format.grid.modis;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNotNull;

public class ModisGridInputFormatTest {

    @Test
    public void testGetTilesLut() throws Exception {
        GeoLutCreator.TileLut tilesLut = ModisGridInputFormat.getTilesLut(new File("C:\\ssd\\modis-geo-luts\\modis-tiles-lut.txt"));
        assertNotNull(tilesLut);
        List<Integer> xVals = new ArrayList<>();
        List<Integer> yVals = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : tilesLut.entrySet()) {
            if (entry.getValue().contains("h19v08")) {
                xVals.add(Integer.parseInt(entry.getKey().split(",")[0]));
                yVals.add(Integer.parseInt(entry.getKey().split(",")[1]));
            }
        }

        Collections.sort(xVals);
        Collections.sort(yVals);

        System.out.println("minX=" + xVals.get(0));
        System.out.println("maxX=" + xVals.get(xVals.size() - 1));
        System.out.println("minY=" + yVals.get(0));
        System.out.println("maxY=" + yVals.get(yVals.size() - 1));
        
//        for (String key : tilesLut.keySet()) {
//            SortedSet<String> inputTiles = new TreeSet<>(tilesLut.get(key));
//            for (String inputTile : inputTiles) {
//                System.out.println(inputTile);
//            }
//        }
    }
}