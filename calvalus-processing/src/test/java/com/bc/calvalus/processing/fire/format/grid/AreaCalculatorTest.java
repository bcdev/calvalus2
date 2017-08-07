package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.AreaCalculator;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;

import java.awt.geom.Rectangle2D;

import static org.junit.Assert.assertEquals;

/**
 * @author Marco Peters
 */
public class AreaCalculatorTest {

    private static final double SIZE_FIRST_LAST_LINE = 1.0813948123130877E8;
    private static final double SIZE_CENTER_LINE = 1.2391557179979538E10;

    private AreaCalculator areaCalculator;

    @Before
    public void setUp() throws Exception {
        CrsGeoCoding gc = new CrsGeoCoding(DefaultGeographicCRS.WGS84, 360, 180, -179.5, 89.5, 1.0, 1.0);
        areaCalculator = new AreaCalculator(gc);
    }

    @Test
    public void testShowSomeResults() throws Exception {
        Product product = ProductIO.readProduct("C:\\ssd\\20160122-ESACCI-L4_FIRE-BA-MSI-fv4.1.nc");
        com.bc.calvalus.processing.fire.format.grid.AreaCalculator areaCalculator = new com.bc.calvalus.processing.fire.format.grid.AreaCalculator(product.getSceneGeoCoding());
        for (int y = 0; y < product.getSceneRasterHeight(); y++) {
//            for (int x = 0; x < product.getSceneRasterWidth(); x++) {
            System.out.println(String.format("pixelY=%s, area=%s", y, areaCalculator.calculatePixelSize(0, y)));
//                System.out.println(String.format("pixelX=%s, pixelY=%s, area=%s", x, y, areaCalculator.calculatePixelSize(x, y)));
//            }
        }

    }

    @Test
    public void testCalculateRectangleSize() throws Exception {
        Rectangle2D.Double currentRect = new Rectangle2D.Double();

        currentRect.setFrameFromDiagonal(-180, 90, -179, 89);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, 90, 1, 89);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, 90, 180, 89);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);

        currentRect.setFrameFromDiagonal(-180, -1, -179, 0);
        assertEquals(SIZE_CENTER_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, -1, 1, 0);
        assertEquals(SIZE_CENTER_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, -1, 180, 0);
        assertEquals(SIZE_CENTER_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);

        currentRect.setFrameFromDiagonal(-180, -89, -179, -90);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, -89, 1, -90);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, -89, 180, -90);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculateRectangleSize(currentRect), 1.0e-6);
    }

    @Test
    public void testCalculatePixelSize() throws Exception {
        Rectangle2D.Double currentRect = new Rectangle2D.Double();

        currentRect.setFrameFromDiagonal(-180, 90, -179, 89);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculatePixelSize(0, 0), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, 90, 1, 89);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculatePixelSize(180, 0), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, 90, 180, 89);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculatePixelSize(359, 0), 1.0e-6);

        currentRect.setFrameFromDiagonal(-180, -1, -179, 0);
        assertEquals(SIZE_CENTER_LINE, areaCalculator.calculatePixelSize(0, 90), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, -1, 1, 0);
        assertEquals(SIZE_CENTER_LINE, areaCalculator.calculatePixelSize(180, 90), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, -1, 180, 0);
        assertEquals(SIZE_CENTER_LINE, areaCalculator.calculatePixelSize(359, 90), 1.0e-6);

        currentRect.setFrameFromDiagonal(-180, -89, -179, -90);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculatePixelSize(0, 179), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, -89, 1, -90);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculatePixelSize(180, 179), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, -89, 180, -90);
        assertEquals(SIZE_FIRST_LAST_LINE, areaCalculator.calculatePixelSize(359, 179), 1.0e-6);
    }
}