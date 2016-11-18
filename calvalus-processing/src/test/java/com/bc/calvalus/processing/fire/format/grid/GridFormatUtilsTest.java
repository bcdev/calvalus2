package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Ignore;
import org.junit.Test;
import ucar.nc2.NetcdfFileWriter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.S2_GRID_PIXELSIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class GridFormatUtilsTest {

    @Ignore
    @Test
    public void acceptanceTestCreateNcFile() throws Exception {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyymmdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault());
        String timeCoverageStart = dtf.format(LocalDate.of(2008, 6, 1).atTime(0, 0, 0));
        String timeCoverageEnd = dtf.format(LocalDate.of(2008, 6, 15).atTime(23, 59, 59));
        NetcdfFileWriter ncFile = GridFormatUtils.createNcFile(".\\" + GridFormatUtils.createMerisFilename("2008", "06", "04.1", true), "v4.1", timeCoverageStart, timeCoverageEnd, 15);
        ncFile.close();
    }

    @Test
    public void testCreateFilename() throws Exception {
        assertEquals("20080607-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", GridFormatUtils.createMerisFilename("2008", "06", "v04.0", true));
        assertEquals("20080622-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", GridFormatUtils.createMerisFilename("2008", "06", "v04.0", false));

        assertEquals("20101007-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc", GridFormatUtils.createMerisFilename("2010", "10", "v04.1", true));
        assertEquals("20101022-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc", GridFormatUtils.createMerisFilename("2010", "10", "v04.1", false));
    }

    @Test
    public void testCreateTimeString() throws Exception {
        String localTimeString = GridFormatUtils.createTimeString(Instant.parse("2007-12-03T10:15:30.00Z"));
        assertEquals("20071203T111530Z", localTimeString);
    }

    @Test
    public void testCreateNiceTimeString() throws Exception {
        String localTimeString = GridFormatUtils.createNiceTimeString(Instant.parse("2007-12-03T10:15:30.00Z"));
        assertEquals("2007-12-03 11:15:30", localTimeString);
    }

    @Test
    public void testGetTargetDimension() throws Exception {
        int size = 500;
        Product a = new Product("a", "t", size, size);
        a.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, size, size, 15, 15, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));
        Product b = new Product("b", "t", size, size);
        b.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, size, size, 15.02, 15.01, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));

        a.addBand("a", "1");
        b.addBand("b", "2");

        GridFormatUtils.ProductSpec ps = GridFormatUtils.getTargetSpec(new Product[]{
                a, b
        });
        assertEquals(610, ps.width);
        assertEquals(555, ps.height);
    }

    @Test
    public void testFilterProducts() throws Exception {
        Product a = new Product("a", "t", 50, 50);
        a.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 50, 50, 10, 10, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));
        Product b = new Product("b", "t", 50, 50);
        b.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 50, 50, 10.05, 10.025, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));
        Product c = new Product("c", "t", 50, 50);
        c.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 50, 50, 16, 15, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE));

        assertArrayEquals(new Product[]{a}, GridFormatUtils.filter("v07h19", new Product[]{a, b, c}, 0, 0));
        assertArrayEquals(new Product[]{b}, GridFormatUtils.filter("v07h19", new Product[]{a, b, c}, 1, 1));

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