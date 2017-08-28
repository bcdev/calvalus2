package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
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
                null,
                geoLookupTables, "765,340");

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
        Product product1 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\burned_2016_12_h31v12.nc");
        Product product2 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\burned_2016_12_h32v12.nc");

        Product lcProduct1 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\h31v12-2010.nc");
        Product lcProduct2 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\h32v12-2010.nc");

        Product areaProduct1 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\areas-h31v12.nc");
        Product areaProduct2 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\areas-h32v12.nc");

        List<ZipFile> geoLookupTables = new ArrayList<>();
        geoLookupTables.add(new ZipFile(new File("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\modis-geo-luts-140x.zip")));
        geoLookupTables.add(new ZipFile(new File("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\modis-geo-luts-141x.zip")));
        geoLookupTables.add(new ZipFile(new File("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\modis-geo-luts-142x.zip")));
        geoLookupTables.add(new ZipFile(new File("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\modis-geo-luts-143x.zip")));

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(
                new Product[]{product1, product2},
                new Product[]{lcProduct1, lcProduct2},
                new Product[]{areaProduct1, areaProduct2},
                geoLookupTables,
                "1408,480");

        dataSource.setDoyFirstOfMonth(335);
        dataSource.setDoyLastOfMonth(366);
        dataSource.setDoyFirstHalf(366 + 7);
        dataSource.setDoySecondHalf(366 + 22);

        for (int y = 0; y < 32; y++) {
            for (int x = 0; x < 32; x++) {
                dataSource.readPixels(x, y);
            }
        }


//        Band band = product1.getBand("classification");
//        assertEquals(0, dataSource.getFloatPixelValue(band, 0), 1E-8);
//        assertEquals(-2, dataSource.getFloatPixelValue(band, 407), 1E-8);
//        assertEquals(-2, dataSource.getFloatPixelValue(band, 380 + 4800 * 7), 1E-8);
//        assertEquals(-2, dataSource.getFloatPixelValue(band, 380 + 4800 * 7), 1E-8);
//        assertEquals(0, dataSource.getFloatPixelValue(band, 379 + 4800 * 7), 1E-8);
//        assertEquals(16, dataSource.getFloatPixelValue(band, 629 + 4800 * 495), 1E-8);
//        assertEquals(0, dataSource.getFloatPixelValue(band, 630 + 4800 * 495), 1E-8);
//
//        assertEquals(0, dataSource.getFloatPixelValue(band, 0), 1E-8);
//        assertEquals(-2, dataSource.getFloatPixelValue(band, 407), 1E-8);
//
//        assertEquals(29, dataSource.getFloatPixelValue(band, 3573 + 4800 * 2826), 1E-8);
    }
}