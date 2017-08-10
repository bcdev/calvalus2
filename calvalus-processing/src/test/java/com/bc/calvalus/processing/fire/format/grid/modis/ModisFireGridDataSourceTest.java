package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.junit.Test;

import java.util.ArrayList;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModisFireGridDataSourceTest {

    @Test
    public void readPixels() throws Exception {
        Product product1 = ProductIO.readProduct("D:\\workspace\\fire-cci\\modis-grid-input\\burned_2001_1_h19v08.nc");
        Product lcProduct1 = ProductIO.readProduct("D:\\workspace\\fire-cci\\modis-grid-input\\h19v08-2000.nc");
        Product product2 = ProductIO.readProduct("D:\\workspace\\fire-cci\\modis-grid-input\\burned_2001_1_h20v08.nc");
        Product lcProduct2 = ProductIO.readProduct("D:\\workspace\\fire-cci\\modis-grid-input\\h20v08-2000.nc");
        ZipFile zipFile = new ZipFile("D:\\workspace\\fire-cci\\modis-grid-input\\modis-geo-luts.zip");
        ArrayList<ZipFile> geoLookupTables = new ArrayList<>();
        geoLookupTables.add(zipFile);
        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(
                new Product[]{product1},
                new Product[]{lcProduct1},
                geoLookupTables, "765,340", null);

        dataSource.setDoyFirstHalf(7);
        dataSource.setDoySecondHalf(22);
        dataSource.setDoyFirstOfMonth(1);
        dataSource.setDoyLastOfMonth(31);

        SourceData sourceData = dataSource.readPixels(0, 0);
    }

    @Test
    public void testGetXCoords() throws Exception {
        String targetTile = "1296,320";
        assertArrayEquals(new String[]{"129x", "130x"}, ModisGridMapper.getXCoords(targetTile));

        targetTile = "800,359";
        assertArrayEquals(new String[]{"080x"}, ModisGridMapper.getXCoords(targetTile));

        targetTile = "816,359";
        assertArrayEquals(new String[]{"081x", "082x"}, ModisGridMapper.getXCoords(targetTile));

        targetTile = "72,359";
        assertArrayEquals(new String[]{"007x"}, ModisGridMapper.getXCoords(targetTile));

        targetTile = "8,359";
        assertArrayEquals(new String[]{"000x", "001x"}, ModisGridMapper.getXCoords(targetTile));

        try {
            ModisGridMapper.getXCoords("15000,346");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ModisGridMapper.getXCoords("1201,320");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testGetUpperLat() throws Exception {
        assertEquals(90, AbstractFireGridDataSource.getUpperLat(0), 1E-5);
        assertEquals(-89.75, AbstractFireGridDataSource.getUpperLat(719), 1E-5);
        assertEquals(-0.25, AbstractFireGridDataSource.getUpperLat(361), 1E-5);
        assertEquals(11.5, AbstractFireGridDataSource.getUpperLat(314), 1E-5);
        assertEquals(64.75, AbstractFireGridDataSource.getUpperLat(101), 1E-5);
        try {
            AbstractFireGridDataSource.getUpperLat(-1);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
        try {
            AbstractFireGridDataSource.getUpperLat(720);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
    }

    @Test
    public void testGetLeftLon() throws Exception {
        assertEquals(-180, AbstractFireGridDataSource.getLeftLon(0), 1E-5);
        assertEquals(179.75, AbstractFireGridDataSource.getLeftLon(1439), 1E-5);
        assertEquals(0.0, AbstractFireGridDataSource.getLeftLon(720), 1E-5);
        assertEquals(-89.75, AbstractFireGridDataSource.getLeftLon(361), 1E-5);
        assertEquals(-101.5, AbstractFireGridDataSource.getLeftLon(314), 1E-5);
        assertEquals(175, AbstractFireGridDataSource.getLeftLon(1420), 1E-5);
        try {
            AbstractFireGridDataSource.getLeftLon(-1);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
        try {
            AbstractFireGridDataSource.getLeftLon(1440);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
    }

    @Test
    public void acceptanceTestCache() throws Exception {
        Product product1 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\burned_2001_1_h19v08.nc");
        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(new Product[]{product1}, null, null, "", null);

        Band band = product1.getBand("classification");
        assertEquals(0, dataSource.getFloatPixelValue(band, 0), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, 407), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, 380 + 4800 * 7), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, 380 + 4800 * 7), 1E-8);
        assertEquals(0, dataSource.getFloatPixelValue(band, 379 + 4800 * 7), 1E-8);
        assertEquals(16, dataSource.getFloatPixelValue(band, 629 + 4800 * 495), 1E-8);
        assertEquals(0, dataSource.getFloatPixelValue(band, 630 + 4800 * 495), 1E-8);

        assertEquals(0, dataSource.getFloatPixelValue(band, 0), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, 407), 1E-8);

        assertEquals(29, dataSource.getFloatPixelValue(band, 3573 + 4800 * 2826), 1E-8);
    }
}