package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
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
    public void name() throws Exception {
        System.out.println((byte) 110);

        Product lc = ProductIO.readProduct("C:\\ssd\\h19v09-2000.nc");
        Band band = lc.getBand("lccs_class");
        ProductData productData = ProductData.createInstance(band.getDataType(), 100 * 100);
        band.readRasterData(3553, 1042, 100, 100, productData);
//        band.readRasterData(0, 0, 4800, 4800, productData);
        System.out.println(productData.getElemIntAt(0));
        System.out.println(productData.getElemIntAt(1));
        System.out.println(productData.getElemIntAt(2));
//        System.out.println(productData.getElemIntAt(2));
    }

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
        String targetTile = "1280,320";
        assertArrayEquals(new String[]{"128x", "129x", "130x", "131x"}, ModisGridMapper.getXCoords(targetTile));

        targetTile = "800,359";
        assertArrayEquals(new String[]{"080x", "081x", "082x", "083x"}, ModisGridMapper.getXCoords(targetTile));

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

        Band band = product1.getBand("classification");
        assertEquals(0, dataSource.getFloatPixelValue(band, "h31v12", 0), 1E-8);
        assertEquals(0, dataSource.getFloatPixelValue(band, "h31v12", 407), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, "h31v12", 379 + 4800 * 17), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, "h31v12", 380 + 4800 * 17), 1E-8);
        assertEquals(0, dataSource.getFloatPixelValue(band, "h31v12", 381 + 4800 * 17), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, "h31v12", 379 + 4800 * 18), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, "h31v12", 379 + 4800 * 19), 1E-8);

        assertEquals(-2, dataSource.getFloatPixelValue(band, "h31v12", 4104 + 4800 * 3920), 1E-8);

    }

    @Test
    public void acceptanceTestCache2() throws Exception {
        Product product3 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\burned_2001_5_h19v08.nc");
        Product lcProduct3 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\h19v08-2005.nc");
        Product areaProduct3 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\areas-h19v08.nc");

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(
                new Product[]{product3},
                new Product[]{lcProduct3},
                new Product[]{areaProduct3},
                null,
                null);

        dataSource.setDoyFirstOfMonth(121);
        dataSource.setDoyLastOfMonth(151);
        dataSource.setDoyFirstHalf(121 + 7);
        dataSource.setDoySecondHalf(151 + 22);

        Band band = product3.getBand("classification");
        assertEquals(-2, dataSource.getFloatPixelValue(band, "h19v08", 3411 + 4800 * 4339), 1E-8);
        assertEquals(0, dataSource.getFloatPixelValue(band, "h19v08", 3412 + 4800 * 4339), 1E-8);

        assertEquals(0, dataSource.getFloatPixelValue(band, "h19v08", 2746 + 4800 * 146), 1E-8);
        assertEquals(127, dataSource.getFloatPixelValue(band, "h19v08", 2747 + 4800 * 146), 1E-8);
        assertEquals(0, dataSource.getFloatPixelValue(band, "h19v08", 2748 + 4800 * 146), 1E-8);
        assertEquals(0, dataSource.getFloatPixelValue(band, "h19v08", 2749 + 4800 * 146), 1E-8);

        assertEquals(-1, dataSource.getFloatPixelValue(band, "h19v08", 4161 + 4800 * 2543), 1E-8);
        assertEquals(0, dataSource.getFloatPixelValue(band, "h19v08", 4162 + 4800 * 2543), 1E-8);

        assertEquals(0, dataSource.getFloatPixelValue(band, "h19v08", 403), 1E-8);
        assertEquals(-2, dataSource.getFloatPixelValue(band, "h19v08", 404), 1E-8);

    }

    @Test
    public void acceptanceTestCache3() throws Exception {
        Product product = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\burned_2002_5_h26v04.nc");
        Product product2 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\burned_2002_5_h26v05.nc");
        Product product3 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\burned_2002_5_h27v05.nc");
        Product lcProduct = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\h26v04-2000.nc");
        Product lcProduct2 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\h26v05-2000.nc");
        Product lcProduct3 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\h27v05-2000.nc");
        Product areaProduct = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\areas-h26v04.nc");
        Product areaProduct2 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\areas-h26v05.nc");
        Product areaProduct3 = ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\areas-h27v05.nc");

        List<ZipFile> geoLookupTables = new ArrayList<>();
        geoLookupTables.add(new ZipFile(new File("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\modis-geo-luts-115x.zip")));
        geoLookupTables.add(new ZipFile(new File("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\modis-geo-luts-116x.zip")));
        geoLookupTables.add(new ZipFile(new File("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\modis-geo-luts-117x.zip")));
        geoLookupTables.add(new ZipFile(new File("D:\\workspace\\fire-cci\\testdata\\modis-grid-input\\modis-geo-luts-118x.zip")));

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(
                new Product[]{product, product2, product3},
                new Product[]{lcProduct, lcProduct2, lcProduct3},
                new Product[]{areaProduct, areaProduct2, areaProduct3},
                geoLookupTables,
                "1152,192");

        dataSource.setDoyFirstOfMonth(121);
        dataSource.setDoyLastOfMonth(151);
        dataSource.setDoyFirstHalf(121 + 7);
        dataSource.setDoySecondHalf(151 + 22);

        for (int y = 0; y < 32; y++) {
            for (int x = 0; x < 32; x++) {
                SourceData sourceData = dataSource.readPixels(x, y);
            }
        }
    }

    @Test
    public void acceptanceTest3() throws Exception {
        Product product = ProductIO.readProduct("c:\\ssd\\burned_2001_12_h10v06.nc");
        Product lcProduct = ProductIO.readProduct("c:\\ssd\\h10v06-2000.nc");
        Product areaProduct = ProductIO.readProduct("c:\\ssd\\areas-h10v06.nc");

        List<ZipFile> geoLookupTables = new ArrayList<>();
        geoLookupTables.add(new ZipFile(new File("c:\\ssd\\modis-geo-luts-039x.zip")));

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(
                new Product[]{product},
                new Product[]{lcProduct},
                new Product[]{areaProduct},
                geoLookupTables,
                "384,256");

        dataSource.setDoyFirstOfMonth(335);
        dataSource.setDoyLastOfMonth(365);
        dataSource.setDoyFirstHalf(335 + 7);
        dataSource.setDoySecondHalf(335 + 22);

        SourceData sourceData = dataSource.readPixels(10, 14);
    }

    @Test
    public void testCreateKey() throws Exception {
        assertEquals(190900261001L, ModisFireGridDataSource.createKey("h19v09", 26, 1001));
        assertEquals(190926100001L, ModisFireGridDataSource.createKey("h19v09", 2610, 1));
    }
}