package com.bc.calvalus.processing.mosaic;


import com.bc.calvalus.processing.utils.GeometryUtils;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.awt.*;
import java.util.List;

import static org.junit.Assert.*;

public class MosaicGridTest {

    @Test
    public void testComputeBounds() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        Rectangle rectangle = mosaicGrid.computeBounds(null);
        assertNotNull(rectangle);
        assertEquals(0, rectangle.x);
        assertEquals(0, rectangle.y);
        assertEquals(370 * 360, rectangle.width);
        assertEquals(370 * 180, rectangle.height);

        Geometry geometry = GeometryUtils.createGeometry("polygon((-180 90, -180 80, -170 80, -170 90, -180 90))");
        rectangle = mosaicGrid.computeBounds(geometry);
        assertNotNull(rectangle);
        assertEquals(0, rectangle.x);
        assertEquals(0, rectangle.y);
        assertEquals(370 * 10 * 36, rectangle.width);
        assertEquals(370 * 10 + 1, rectangle.height);

        geometry = GeometryUtils.createGeometry("polygon((-179.5 89.5, -179.5 80.5, -170.5 80.5, -170.5 89.5, -179.5 89.5))");
        rectangle = mosaicGrid.computeBounds(geometry);
        assertNotNull(rectangle);
        assertEquals(185 - 1, rectangle.x);
        assertEquals(185 - 1, rectangle.y);
        assertEquals(370 * 9 + 2, rectangle.width);
        assertEquals(370 * 9 + 1, rectangle.height);
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
        Geometry geometry;
        List<Point> tilePointIndices = mosaicGrid.getTilePointIndicesGlobal();
        TileIndexWritable[] tileIndices = mosaicGrid.getTileIndices(tilePointIndices);
        assertNotNull(tileIndices);
        assertEquals(360 * 180, tileIndices.length);
        assertEquals(new TileIndexWritable(0, 0, 0, 0), tileIndices[0]);
        assertEquals(new TileIndexWritable(0, 0, 1, 0), tileIndices[1]);
        assertEquals(new TileIndexWritable(0, 0, 2, 0), tileIndices[2]);
        assertEquals(new TileIndexWritable(0, 0, 3, 0), tileIndices[3]);
        assertEquals(new TileIndexWritable(0, 0, 4, 0), tileIndices[4]);
        assertEquals(new TileIndexWritable(1, 0, 5, 0), tileIndices[5]);
        assertEquals(new TileIndexWritable(2, 0, 10, 0), tileIndices[10]);
        assertEquals(new TileIndexWritable(0, 0, 0, 1), tileIndices[360]);
        assertEquals(new TileIndexWritable(0, 1, 0, 5), tileIndices[5 * 360]);

        geometry = GeometryUtils.createGeometry("polygon((-2 46, -2 40, 1 40, 1 46, -2 46))");
        tilePointIndices = mosaicGrid.getTilePointIndicesFromGeometry(geometry);
        tileIndices = mosaicGrid.getTileIndices(tilePointIndices);
        assertNotNull(tileIndices);
        assertEquals(3 * 6, tileIndices.length);

        assertEquals(new TileIndexWritable(35, 8, 178, (90 - 46)), tileIndices[0]);
        assertEquals(new TileIndexWritable(35, 8, 179, (90 - 46)), tileIndices[1]);
        assertEquals(new TileIndexWritable(36, 8, 180, (90 - 46)), tileIndices[2]);

        assertEquals(new TileIndexWritable(35, 9, 178, (90 - 45)), tileIndices[3]);
        assertEquals(new TileIndexWritable(35, 9, 179, (90 - 45)), tileIndices[4]);
        assertEquals(new TileIndexWritable(36, 9, 180, (90 - 45)), tileIndices[5]);

        geometry = GeometryUtils.createGeometry("polygon((-2 46, -2 45, 0 45, 0 40, 1 40, 1 46, -2 46))");
        tilePointIndices = mosaicGrid.getTilePointIndicesFromGeometry(geometry);
        tileIndices = mosaicGrid.getTileIndices(tilePointIndices);
        assertNotNull(tileIndices);
        assertEquals(8, tileIndices.length);
        assertEquals(new TileIndexWritable(35, 8, 178, (90 - 46)), tileIndices[0]);
        assertEquals(new TileIndexWritable(35, 8, 179, (90 - 46)), tileIndices[1]);
        assertEquals(new TileIndexWritable(36, 8, 180, (90 - 46)), tileIndices[2]);

        assertEquals(new TileIndexWritable(36, 9, 180, (90 - 45)), tileIndices[3]);
        assertEquals(new TileIndexWritable(36, 9, 180, (90 - 44)), tileIndices[4]);
        assertEquals(new TileIndexWritable(36, 9, 180, (90 - 43)), tileIndices[5]);
        assertEquals(new TileIndexWritable(36, 9, 180, (90 - 42)), tileIndices[6]);
        assertEquals(new TileIndexWritable(36, 9, 180, (90 - 41)), tileIndices[7]);
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
    public void testDegreeToTileX() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        assertEquals(0, mosaicGrid.degreeToTileX(-180.0));
        assertEquals(1, mosaicGrid.degreeToTileX(-179.0));
        assertEquals(180, mosaicGrid.degreeToTileX(0.0));
        assertEquals(360, mosaicGrid.degreeToTileX(180.0));
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
    public void testDegreeToTileY() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        assertEquals(0, mosaicGrid.degreeToTileY(90.0), 1e-5);
        assertEquals(1, mosaicGrid.degreeToTileY(89.0), 1e-5);
        assertEquals(90, mosaicGrid.degreeToTileY(0.0), 1e-5);
        assertEquals(180, mosaicGrid.degreeToTileY(-90.0), 1e-5);
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
        MosaicGrid mosaicGrid = new MosaicGrid(5, 180 / 5, 370 * 5);

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

    @Test
    public void testGetter() throws Exception {
        MosaicGrid mosaicGrid = new MosaicGrid();
        assertEquals(360, mosaicGrid.getNumTileX());
        assertEquals(180, mosaicGrid.getNumTileY());
        assertEquals(72, mosaicGrid.getNumMacroTileX());
        assertEquals(36, mosaicGrid.getNumMacroTileY());

        mosaicGrid = new MosaicGrid(10, 180, 360);
        assertEquals(360, mosaicGrid.getNumTileX());
        assertEquals(180, mosaicGrid.getNumTileY());
        assertEquals(36, mosaicGrid.getNumMacroTileX());
        assertEquals(18, mosaicGrid.getNumMacroTileY());
    }

    @Test
    public void testSaveToConfiguration() throws Exception {
        Configuration configuration = new Configuration();
        assertNull(configuration.get("calvalus.mosaic.macroTileSize"));
        assertNull(configuration.get("calvalus.mosaic.numTileY"));
        assertNull(configuration.get("calvalus.mosaic.tileSize"));

        MosaicGrid mosaicGrid = new MosaicGrid();
        mosaicGrid.saveToConfiguration(configuration);
        assertNotNull(configuration.get("calvalus.mosaic.macroTileSize"));
        assertNotNull(configuration.get("calvalus.mosaic.numTileY"));
        assertNotNull(configuration.get("calvalus.mosaic.tileSize"));

        assertEquals("5", configuration.get("calvalus.mosaic.macroTileSize"));
        assertEquals("180", configuration.get("calvalus.mosaic.numTileY"));
        assertEquals("370", configuration.get("calvalus.mosaic.tileSize"));

        mosaicGrid = new MosaicGrid(6, 18, 20);
        mosaicGrid.saveToConfiguration(configuration);

        assertEquals("6", configuration.get("calvalus.mosaic.macroTileSize"));
        assertEquals("18", configuration.get("calvalus.mosaic.numTileY"));
        assertEquals("20", configuration.get("calvalus.mosaic.tileSize"));

    }
}
