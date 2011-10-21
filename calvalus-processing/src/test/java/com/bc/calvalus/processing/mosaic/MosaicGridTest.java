package com.bc.calvalus.processing.mosaic;


import com.bc.calvalus.processing.JobUtils;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;

import java.awt.Point;
import java.awt.Rectangle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void testGetTileIndices() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        Geometry geometry = null;
        Point[] tileIndices = mosaicGrid.getTileIndices(geometry);
        assertNotNull(tileIndices);
        assertEquals(360 * 180, tileIndices.length);
        assertEquals(new Point(0, 0), tileIndices[0]);
        assertEquals(new Point(1, 0), tileIndices[1]);

        geometry = JobUtils.createGeometry("polygon((-2 46, -2 40, 1 40, 1 46, -2 46))");
        tileIndices = mosaicGrid.getTileIndices(geometry);
        assertNotNull(tileIndices);
        assertEquals(3 * 6, tileIndices.length);

        assertEquals(new Point(178, (90 - 46)), tileIndices[0]);
        assertEquals(new Point(179, (90 - 46)), tileIndices[1]);
        assertEquals(new Point(180, (90 - 46)), tileIndices[2]);

        assertEquals(new Point(178, (90 - 45)), tileIndices[3]);
        assertEquals(new Point(179, (90 - 45)), tileIndices[4]);
        assertEquals(new Point(180, (90 - 45)), tileIndices[5]);


        geometry = JobUtils.createGeometry("polygon((-2 46, -2 45, 0 45, 0 40, 1 40, 1 46, -2 46))");
        tileIndices = mosaicGrid.getTileIndices(geometry);
        assertNotNull(tileIndices);
        assertEquals(8, tileIndices.length);
        assertEquals(new Point(178, (90 - 46)), tileIndices[0]);
        assertEquals(new Point(179, (90 - 46)), tileIndices[1]);
        assertEquals(new Point(180, (90 - 46)), tileIndices[2]);

        assertEquals(new Point(180, (90 - 45)), tileIndices[3]);
        assertEquals(new Point(180, (90 - 44)), tileIndices[4]);
        assertEquals(new Point(180, (90 - 43)), tileIndices[5]);
        assertEquals(new Point(180, (90 - 42)), tileIndices[6]);
        assertEquals(new Point(180, (90 - 41)), tileIndices[7]);
    }

    @Test
    public void testGetTileIndices_5degrees() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid(180 / 5, 370 * 5);
        Geometry geometry = null;
        Point[] tileIndices = mosaicGrid.getTileIndices(geometry);
        assertNotNull(tileIndices);
        assertEquals(72 * 36, tileIndices.length);
        assertEquals(new Point(0, 0), tileIndices[0]);
        assertEquals(new Point(1, 0), tileIndices[1]);
    }

    @Test
    public void testTileXToDegree() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        assertEquals(-180.0, mosaicGrid.tileXToDegree(0), 1e-5);
        assertEquals(-179.0, mosaicGrid.tileXToDegree(1), 1e-5);
        assertEquals(0.0, mosaicGrid.tileXToDegree(180), 1e-5);
        assertEquals(180.0, mosaicGrid.tileXToDegree(360), 1e-5);
    }

    @Test
    public void testTileYToDegree() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        assertEquals(90.0, mosaicGrid.tileYToDegree(0), 1e-5);
        assertEquals(89.0, mosaicGrid.tileYToDegree(1), 1e-5);
        assertEquals(0.0, mosaicGrid.tileYToDegree(90), 1e-5);
        assertEquals(-90.0, mosaicGrid.tileYToDegree(180), 1e-5);
    }

    @Test
    public void testGetTileGeometry() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();

        assertTileGeometry(mosaicGrid, 0, 0, -180.0, -179.0, 89.0, 90.0);
        assertTileGeometry(mosaicGrid, 180, 90, 0.0, 1.0, -1.0, 0.0);
        assertTileGeometry(mosaicGrid, 10, 10, -170.0, -169.0, 79.0, 80.0);
    }

    @Test
    public void testGetTileGeometry_5degree() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid(180 / 5, 370 * 5);

        assertTileGeometry(mosaicGrid, 0, 0, -180.0, -175.0, 85.0, 90.0);
        assertTileGeometry(mosaicGrid, 36, 18, 0.0, 5.0, -5.0, 0.0);
        assertTileGeometry(mosaicGrid, 10, 10, -130.0, -125.0, 35.0, 40.0);
    }

    private void assertTileGeometry(MosaicGrid mosaicGrid, int tileX, int tileY, double minX, double maxX, double minY, double maxY) {
        Geometry tileGeometry = mosaicGrid.getTileGeometry(tileX, tileY);
        assertNotNull(tileGeometry);
        assertTrue(tileGeometry.isRectangle());
        Envelope envelope = tileGeometry.getEnvelopeInternal();
        assertEquals("minX", minX, envelope.getMinX(), 1e-5);
        assertEquals("maxX", maxX, envelope.getMaxX(), 1e-5);
        assertEquals("minY", minY, envelope.getMinY(), 1e-5);
        assertEquals("maxY", maxY, envelope.getMaxY(), 1e-5);
    }
}
