package com.bc.calvalus.processing.fire.format.grid;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class GridInputFormatTest {

    @Test
    public void getLcInputPath() throws Exception {
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2005-v04h01.nc",
                GridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2006/BA_PIX_MER_v04h01_200606_v4.0.tif"));
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2005-v12h11.nc",
                GridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2005/BA_PIX_MER_v12h11_200506_v4.0.tif"));
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2010-v12h11.nc",
                GridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2008/BA_PIX_MER_v12h11_200806_v4.0.tif"));
    }

    @Test
    public void getSrInputPath() throws Exception {
        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2006/v04h01/2006/2006-06-*/CCI-Fire-*.nc",
                GridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2006/BA_PIX_MER_v04h01_200606_v4.0.tif"));

        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2005/v12h11/2005/2005-06-*/CCI-Fire-*.nc",
                GridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2005/BA_PIX_MER_v12h11_200506_v4.0.tif"));

        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/v12h11/2008/2008-03-*/CCI-Fire-*.nc",
                GridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2008/BA_PIX_MER_v12h11_200803_v4.0.tif"));
    }

    @Test
    public void testGetTile() throws Exception {
        assertEquals("v03h08",
                GridInputFormat.getTile("hdfs://calvalus/calvalus/projects/fire/meris-ba/$year/BA_PIX_MER_v03h08_$year$month_v4.0.tif"));
    }

    @Test
    public void testGetMissingTiles() throws Exception {
        List<String> usedTiles = new ArrayList<>();
        usedTiles.add("v03h08");
        usedTiles.add("v04h08");
        usedTiles.add("v05h08");
        List<String> missingTiles = GridInputFormat.getMissingTiles(usedTiles);
        assertEquals(645, missingTiles.size());
        assertTrue(missingTiles.contains("v03h09"));
        assertTrue(missingTiles.contains("v04h09"));
        assertTrue(missingTiles.contains("v05h09"));
        assertFalse(missingTiles.contains("v03h08"));
        assertFalse(missingTiles.contains("v04h08"));
        assertFalse(missingTiles.contains("v05h08"));
    }
}