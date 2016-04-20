package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;

import java.awt.geom.Rectangle2D;

import static org.junit.Assert.assertEquals;

/**
 * @author Marco Peters
 */
public class AreaCalculatorTest {

    private AreaCalculator areaCalculator;

    @Before
    public void setUp() throws Exception {
        CrsGeoCoding gc = new CrsGeoCoding(DefaultGeographicCRS.WGS84, 360, 180, -179.5, 89.5, 1.0, 1.0);
        areaCalculator = new AreaCalculator(gc);
    }

    @Test
    public void testCalculate() throws Exception {
        Rectangle2D.Double currentRect = new Rectangle2D.Double();

        double upperLineAreaSize = 1.0813948123130877E8;
        currentRect.setFrameFromDiagonal(-180, 90, -179, 89);
        assertEquals(upperLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, 90, 1, 89);
        assertEquals(upperLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, 90, 180, 89);
        assertEquals(upperLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);

        double centerLineAreaSize = 1.2391557179979538E10;
        currentRect.setFrameFromDiagonal(-180, -1, -179, 0);
        assertEquals(centerLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, -1, 1, 0);
        assertEquals(centerLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, -1, 180, 0);
        assertEquals(centerLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);

        double lowerLineAreaSize = 1.0813948123130877E8;
        currentRect.setFrameFromDiagonal(-180, -89, -179, -90);
        assertEquals(lowerLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(0, -89, 1, -90);
        assertEquals(lowerLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);
        currentRect.setFrameFromDiagonal(179, -89, 180, -90);
        assertEquals(lowerLineAreaSize, areaCalculator.calculateSize(currentRect), 1.0e-6);
    }

    @Test
    public void testCreateGeoRectangleForPixel() throws Exception {
        Rectangle2D.Double currentRect = new Rectangle2D.Double();

        currentRect.setFrameFromDiagonal(-180, 90, -179, 89);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(0, 0));
        currentRect.setFrameFromDiagonal(0, 90, 1, 89);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(180, 0));
        currentRect.setFrameFromDiagonal(179, 90, 180, 89);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(359, 0));

        currentRect.setFrameFromDiagonal(-180, -1, -179, 0);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(0, 90));
        currentRect.setFrameFromDiagonal(0, -1, 1, 0);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(180, 90));
        currentRect.setFrameFromDiagonal(179, -1, 180, 0);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(359, 90));

        currentRect.setFrameFromDiagonal(-180, -89, -179, -90);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(0, 179));
        currentRect.setFrameFromDiagonal(0, -89, 1, -90);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(180, 179));
        currentRect.setFrameFromDiagonal(179, -89, 180, -90);
        assertEquals(currentRect, areaCalculator.createGeoRectangleForPixel(359, 179));
    }

}