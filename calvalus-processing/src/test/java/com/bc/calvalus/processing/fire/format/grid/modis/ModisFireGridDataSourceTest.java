package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Test;

import java.util.zip.ZipFile;

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
        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(
                new Product[]{product1},
                new Product[]{lcProduct1},
                new ZipFile("D:\\workspace\\fire-cci\\modis-grid-input\\modis-geo-luts.zip"), "765,340");

        dataSource.setDoyFirstHalf(7);
        dataSource.setDoySecondHalf(22);
        dataSource.setDoyFirstOfMonth(1);
        dataSource.setDoyLastOfMonth(31);

        SourceData sourceData = dataSource.readPixels(0, 0);
    }

    @Test
    public void testGetXCoord() throws Exception {
        String targetTile = "1486,359";
        assertEquals("148x", ModisGridMapper.getXCoord(targetTile));

        targetTile = "800,359";
        assertEquals("080x", ModisGridMapper.getXCoord(targetTile));

        targetTile = "810,359";
        assertEquals("081x", ModisGridMapper.getXCoord(targetTile));

        targetTile = "70,359";
        assertEquals("007x", ModisGridMapper.getXCoord(targetTile));

        targetTile = "5,359";
        assertEquals("000x", ModisGridMapper.getXCoord(targetTile));

        try {
            ModisGridMapper.getXCoord("15000,346");
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
}