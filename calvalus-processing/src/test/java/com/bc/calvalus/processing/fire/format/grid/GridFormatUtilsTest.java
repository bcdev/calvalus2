package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.time.Instant;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.S2_GRID_PIXELSIZE;
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

//    @Test
//    public void testFilterProducts() throws Exception {
//        Product a = new Product("a", "t", 50, 50);
//        Product b = new Product("b", "t", 50, 50);
//        Product c = new Product("c", "t", 50, 50);
//        moveTo(a, 30, -2);
//        moveTo(b, 30.248, -2);
//        moveTo(c, 30.25, -5);
//
//        assertArrayEquals(new Product[]{a, b}, GridFormatUtils.filter("x210y88", new Product[]{a, b, c}, 0, 0));
//        assertArrayEquals(new Product[]{b}, GridFormatUtils.filter("x210y88", new Product[]{a, b, c}, 1, 0));
//        assertArrayEquals(new Product[]{c}, GridFormatUtils.filter("x210y84", new Product[]{a, b, c}, 1, 3));
//        assertArrayEquals(new Product[]{}, GridFormatUtils.filter("x210y84", new Product[]{a, b, c}, 1, 5));
//
//        try {
//            GridFormatUtils.filter("x210y88", new Product[]{a, b, c}, -1, 8);
//            fail();
//        } catch (IllegalArgumentException e) {
//             ok
//        }
//
//    }

    private static void moveTo(Product p, double east, double north) throws FactoryException, TransformException {
        p.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 50, 50, east, north, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));
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