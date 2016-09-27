package com.bc.calvalus.processing.fire.format;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommonUtilsTest {

    @Test
    public void testGetTile() throws Exception {
        assertEquals("v03h08",
                CommonUtils.getMerisTile("hdfs://calvalus/calvalus/projects/fire/meris-ba/$year/BA_PIX_MER_v03h08_$year$month_v4.0.tif"));
    }

    @Test
    public void testGetMissingTiles() throws Exception {
        List<String> usedTiles = new ArrayList<>();
        usedTiles.add("v03h08");
        usedTiles.add("v04h08");
        usedTiles.add("v05h08");
        List<String> missingTiles = CommonUtils.getMissingTiles(usedTiles);
        assertEquals(645, missingTiles.size());
        assertTrue(missingTiles.contains("v03h09"));
        assertTrue(missingTiles.contains("v04h09"));
        assertTrue(missingTiles.contains("v05h09"));
        assertFalse(missingTiles.contains("v03h08"));
        assertFalse(missingTiles.contains("v04h08"));
        assertFalse(missingTiles.contains("v05h08"));
    }

    @Test
    public void filterPathNames() throws Exception {
        List<String> filteredPathNames = CommonUtils.filterPathNames(Arrays.asList(
                "BA_PIX_MER_v04h01_200606_v4.1.tif",
                "BA_PIX_MER_v04h05_200606_v4.1.tif",
                "BA_PIX_MER_v04h06_200606_v4.1.tif",
                "BA_PIX_MER_v04h01_200606_v4.0.tif",
                "BA_PIX_MER_v04h05_200606_v4.0.tif",
                "BA_PIX_MER_v04h06_200606_v4.0.tif",
                "BA_PIX_MER_v04h07_200606_v4.0.tif")
        );

        String[] filteredPathNamesArray = filteredPathNames.toArray(new String[0]);
        String[] expected = {
                "BA_PIX_MER_v04h01_200606_v4.1.tif",
                "BA_PIX_MER_v04h05_200606_v4.1.tif",
                "BA_PIX_MER_v04h06_200606_v4.1.tif",
                "BA_PIX_MER_v04h07_200606_v4.0.tif",
        };

        Arrays.sort(filteredPathNamesArray);
        Arrays.sort(expected);
        assertArrayEquals(expected, filteredPathNamesArray);
    }

}