package com.bc.calvalus.processing.fire.format.grid.modis;

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
        Product product = ProductIO.readProduct("D:\\workspace\\fire-cci\\modis-grid-input\\burned_2001_1_h19v08.nc");
        Product lcProduct = ProductIO.readProduct("D:\\workspace\\fire-cci\\modis-grid-input\\h19v08-2000.nc");
        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(
                new Product[]{product},
                new Product[]{lcProduct},
                new ZipFile("C:\\ssd\\modis-geo-luts\\modis-geo-luts.zip"));

        SourceData sourceData = dataSource.readPixels(765, 348);
    }

    @Test
    public void testGetUpperLat() throws Exception {
        assertEquals(90, ModisFireGridDataSource.getUpperLat(0), 1E-5);
        assertEquals(-89.75, ModisFireGridDataSource.getUpperLat(719), 1E-5);
        assertEquals(-0.25, ModisFireGridDataSource.getUpperLat(361), 1E-5);
        assertEquals(11.5, ModisFireGridDataSource.getUpperLat(314), 1E-5);
        assertEquals(64.75, ModisFireGridDataSource.getUpperLat(101), 1E-5);
        try {
            ModisFireGridDataSource.getUpperLat(-1);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
        try {
            ModisFireGridDataSource.getUpperLat(720);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
    }

    @Test
    public void testGetLeftLon() throws Exception {
        assertEquals(-180, ModisFireGridDataSource.getLeftLon(0), 1E-5);
        assertEquals(179.75, ModisFireGridDataSource.getLeftLon(1439), 1E-5);
        assertEquals(0.0, ModisFireGridDataSource.getLeftLon(720), 1E-5);
        assertEquals(-89.75, ModisFireGridDataSource.getLeftLon(361), 1E-5);
        assertEquals(-101.5, ModisFireGridDataSource.getLeftLon(314), 1E-5);
        assertEquals(175, ModisFireGridDataSource.getLeftLon(1420), 1E-5);
        try {
            ModisFireGridDataSource.getLeftLon(-1);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
        try {
            ModisFireGridDataSource.getLeftLon(1440);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid value"));
        }
    }
}