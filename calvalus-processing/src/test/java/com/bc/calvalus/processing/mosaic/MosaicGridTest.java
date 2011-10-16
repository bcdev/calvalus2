package com.bc.calvalus.processing.mosaic;


import com.bc.calvalus.processing.JobUtils;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;
import sun.plugin.dom.css.Rect;

import java.awt.Rectangle;

import static org.junit.Assert.*;

public class MosaicGridTest {

    @Test
    public void testComputeRegion() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        Rectangle rectangle = mosaicGrid.computeRegion(null);
        assertNotNull(rectangle);
        assertEquals(0, rectangle.x);
        assertEquals(0, rectangle.y);
        assertEquals(370 * 360, rectangle.width);
        assertEquals(370 * 180, rectangle.height);

        Geometry geometry = JobUtils.createGeometry("polygon((-180 90, -180 80, -170 80, -170 90, -180 90))");
        rectangle = mosaicGrid.computeRegion(geometry);
        assertNotNull(rectangle);
        assertEquals(0, rectangle.x);
        assertEquals(0, rectangle.y);
        assertEquals(370 * 10, rectangle.width);
        assertEquals(370 * 10, rectangle.height);

        geometry = JobUtils.createGeometry("polygon((-179.5 89.5, -179.5 80.5, -170.5 80.5, -170.5 89.5, -179.5 89.5))");
        rectangle = mosaicGrid.computeRegion(geometry);
        assertNotNull(rectangle);
        assertEquals(185, rectangle.x);
        assertEquals(185, rectangle.y);
        assertEquals(370 * 9, rectangle.width);
        assertEquals(370 * 9, rectangle.height);

    }

    @Test
    public void testAlignToTileGrid() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        Rectangle rectangle = new Rectangle(0, 0, 370 * 360, 370 * 180);
        Rectangle aligned = mosaicGrid.alignToTileGrid(rectangle);
        assertNotNull(aligned);
        assertEquals(0, aligned.x);
        assertEquals(0, aligned.y);
        assertEquals(370 * 360, aligned.width);
        assertEquals(370 * 180, aligned.height);

        rectangle = new Rectangle(0, 0, 370 * 10, 370 * 10);
        aligned = mosaicGrid.alignToTileGrid(rectangle);
        assertNotNull(aligned);
        assertEquals(0, aligned.x);
        assertEquals(0, aligned.y);
        assertEquals(370 * 10, aligned.width);
        assertEquals(370 * 10, aligned.height);

        rectangle = new Rectangle(185, 185, 370 * 9, 370 * 9);
        aligned = mosaicGrid.alignToTileGrid(rectangle);
        assertNotNull(aligned);
        assertEquals(0, aligned.x);
        assertEquals(0, aligned.y);
        assertEquals(370 * 10, aligned.width);
        assertEquals(370 * 10, aligned.height);
    }
}
