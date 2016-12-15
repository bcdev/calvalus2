package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.S2_GRID_PIXELSIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class GridFormatUtilsTest {

    @Test
    public void testCreateTimeString() throws Exception {
        String localTimeString = NcFileFactory.createTimeString(Instant.parse("2007-12-03T10:15:30.00Z"));
        assertEquals("20071203T111530Z", localTimeString);
    }

    @Test
    public void testCreateNiceTimeString() throws Exception {
        String localTimeString = NcFileFactory.createNiceTimeString(Instant.parse("2007-12-03T10:15:30.00Z"));
        assertEquals("2007-12-03 11:15:30", localTimeString);
    }

    @Test
    public void testFilterProducts() throws Exception {
        Product a = new Product("a", "t", 50, 50);
        a.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 50, 50, 10, 10, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));
        Product b = new Product("b", "t", 50, 50);
        b.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 50, 50, 10.25, 9.5, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));
        Product c = new Product("c", "t", 50, 50);
        c.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 50, 50, 16, 15, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));

        assertArrayEquals(new Product[]{a}, GridFormatUtils.filter("v40h95", new Product[]{a, b, c}, 0, 0));
        assertArrayEquals(new Product[]{b}, GridFormatUtils.filter("v40h95", new Product[]{a, b, c}, 1, 1));
    }

    @Ignore
    @Test
    public void acceptanceTestCreateActualOutput() throws Exception {
        Product product = ProductIO.readProduct("C:\\temp\\20100607-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc");
        double burnedArea = ProductUtils.getGeophysicalSampleAsDouble(product.getBand("burned_area"), 819, 395, 0);
        assertEquals(10231083, burnedArea, 1E-5f);

        double burnedAreaInVegClasses = 0.0;
        for (int i = 1; i <= 18; i++) {
            burnedAreaInVegClasses += ProductUtils.getGeophysicalSampleAsDouble(product.getBand("burned_area_in_vegetation_class_vegetation_class" + i), 819, 395, 0);
        }

        assertEquals(10231083, burnedAreaInVegClasses, 1E-5f);
    }
}