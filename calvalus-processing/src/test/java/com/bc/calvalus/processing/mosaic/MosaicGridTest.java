package com.bc.calvalus.processing.mosaic;


import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.*;
import java.awt.geom.GeneralPath;
import java.awt.geom.PathIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.bc.calvalus.processing.utils.GeometryUtils.parseWKT;
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
        assertEquals(370 * 10 + 2, rectangle.height);

        geometry = GeometryUtils.createGeometry("polygon((-179.5 89.5, -179.5 80.5, -170.5 80.5, -170.5 89.5, -179.5 89.5))");
        rectangle = mosaicGrid.computeBounds(geometry);
        assertNotNull(rectangle);
        assertEquals(185 - 1, rectangle.x);
        assertEquals(185 - 1, rectangle.y);
        assertEquals(370 * 9 + 3, rectangle.width);
        assertEquals(370 * 9 + 2, rectangle.height);
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
        MosaicGrid mosaicGrid = new MosaicGrid(5, 180 / 5, 370 * 5, true, 8, 0, null);

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

        mosaicGrid = new MosaicGrid(10, 180, 360, true, 8, 0, null);
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

        mosaicGrid = new MosaicGrid(6, 18, 20, true, 8, 0, null);
        mosaicGrid.saveToConfiguration(configuration);

        assertEquals("6", configuration.get("calvalus.mosaic.macroTileSize"));
        assertEquals("18", configuration.get("calvalus.mosaic.numTileY"));
        assertEquals("20", configuration.get("calvalus.mosaic.tileSize"));

    }

    @Test
    public void testGetPartitionFull() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("calvalus.mosaic.tileSize", "2700");
        configuration.set("calvalus.mosaic.numTileY", "360");
        configuration.set("calvalus.mosaic.macroTileSize", "5");
        configuration.set("calvalus.mosaic.withIntersectionCheck", "true");
        configuration.set("calvalus.mosaic.maxReducers", "72");
        configuration.set("calvalus.mosaic.numXPartitions", "72");
        configuration.set("calvalus.mosaic.regionGeometry", "POLYGON((1 -19,1 19,9 19,9 -19,1 -19))");
        final MosaicGrid mosaicGrid = MosaicGrid.create(configuration);
        assertEquals(2700, mosaicGrid.getTileSize());
        assertEquals(5, mosaicGrid.getMacroTileSize());
        assertEquals(360, mosaicGrid.getNumTileY());
        assertEquals(720, mosaicGrid.getNumTileX());
        assertEquals(72, mosaicGrid.getNumMacroTileY());
        assertEquals(144, mosaicGrid.getNumMacroTileX());
        assertEquals(28, mosaicGrid.macroTileRegion.y);
        assertEquals(72, mosaicGrid.macroTileRegion.x);
        assertEquals(16, mosaicGrid.macroTileRegion.height);
        assertEquals(4, mosaicGrid.macroTileRegion.width);
        assertEquals(1, mosaicGrid.macroTileStepY);
        assertEquals(1, mosaicGrid.macroTileStepX);
        assertEquals(4, mosaicGrid.macroTileCountX);
        assertEquals(64, mosaicGrid.getNumReducers());
        assertEquals(32, mosaicGrid.getPartition(72,36));
        assertEquals(30, mosaicGrid.getPartition(70,36));
        assertEquals(28, mosaicGrid.getPartition(72,35));
    }

    @Test
    public void testGetPartitionPartX() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("calvalus.mosaic.tileSize", "2700");
        configuration.set("calvalus.mosaic.numTileY", "360");
        configuration.set("calvalus.mosaic.macroTileSize", "5");
        configuration.set("calvalus.mosaic.withIntersectionCheck", "true");
        configuration.set("calvalus.mosaic.maxReducers", "36");
        configuration.set("calvalus.mosaic.numXPartitions", "36");
        configuration.set("calvalus.mosaic.regionGeometry", "POLYGON((1 -19,1 19,9 19,9 -19,1 -19))");
        final MosaicGrid mosaicGrid = MosaicGrid.create(configuration);
        assertEquals(2700, mosaicGrid.getTileSize());
        assertEquals(5, mosaicGrid.getMacroTileSize());
        assertEquals(360, mosaicGrid.getNumTileY());
        assertEquals(720, mosaicGrid.getNumTileX());
        assertEquals(72, mosaicGrid.getNumMacroTileY());
        assertEquals(144, mosaicGrid.getNumMacroTileX());
        assertEquals(28, mosaicGrid.macroTileRegion.y);
        assertEquals(72, mosaicGrid.macroTileRegion.x);
        assertEquals(16, mosaicGrid.macroTileRegion.height);
        assertEquals(4, mosaicGrid.macroTileRegion.width);
        assertEquals(1, mosaicGrid.macroTileStepY);
        assertEquals(2, mosaicGrid.macroTileStepX);
        assertEquals(2, mosaicGrid.macroTileCountX);
        assertEquals(32, mosaicGrid.getNumReducers());
        assertEquals(16, mosaicGrid.getPartition(72,36));
        assertEquals(15, mosaicGrid.getPartition(70,36));
        assertEquals(14, mosaicGrid.getPartition(72,35));
    }

    @Test
    public void testGetPartitionFullY() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("calvalus.mosaic.tileSize", "2700");
        configuration.set("calvalus.mosaic.numTileY", "360");
        configuration.set("calvalus.mosaic.macroTileSize", "5");
        configuration.set("calvalus.mosaic.withIntersectionCheck", "true");
        configuration.set("calvalus.mosaic.maxReducers", "18");
        configuration.set("calvalus.mosaic.numXPartitions", "18");
        configuration.set("calvalus.mosaic.regionGeometry", "POLYGON((1 -19,1 19,9 19,9 -19,1 -19))");
        final MosaicGrid mosaicGrid = MosaicGrid.create(configuration);
        assertEquals(2700, mosaicGrid.getTileSize());
        assertEquals(5, mosaicGrid.getMacroTileSize());
        assertEquals(360, mosaicGrid.getNumTileY());
        assertEquals(720, mosaicGrid.getNumTileX());
        assertEquals(72, mosaicGrid.getNumMacroTileY());
        assertEquals(144, mosaicGrid.getNumMacroTileX());
        assertEquals(28, mosaicGrid.macroTileRegion.y);
        assertEquals(72, mosaicGrid.macroTileRegion.x);
        assertEquals(16, mosaicGrid.macroTileRegion.height);
        assertEquals(4, mosaicGrid.macroTileRegion.width);
        assertEquals(1, mosaicGrid.macroTileStepY);
        assertEquals(4, mosaicGrid.macroTileStepX);
        assertEquals(1, mosaicGrid.macroTileCountX);
        assertEquals(16, mosaicGrid.getNumReducers());
        assertEquals(8, mosaicGrid.getPartition(72,36));
        assertEquals(8, mosaicGrid.getPartition(70,36));
        assertEquals(7, mosaicGrid.getPartition(72,35));
    }

    @Test
    public void testGetPartitionPartY() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("calvalus.mosaic.tileSize", "2700");
        configuration.set("calvalus.mosaic.numTileY", "360");
        configuration.set("calvalus.mosaic.macroTileSize", "5");
        configuration.set("calvalus.mosaic.withIntersectionCheck", "true");
        configuration.set("calvalus.mosaic.maxReducers", "9");
        configuration.set("calvalus.mosaic.numXPartitions", "9");
        configuration.set("calvalus.mosaic.regionGeometry", "POLYGON((1 -19,1 19,9 19,9 -19,1 -19))");
        final MosaicGrid mosaicGrid = MosaicGrid.create(configuration);
        assertEquals(2700, mosaicGrid.getTileSize());
        assertEquals(5, mosaicGrid.getMacroTileSize());
        assertEquals(360, mosaicGrid.getNumTileY());
        assertEquals(720, mosaicGrid.getNumTileX());
        assertEquals(72, mosaicGrid.getNumMacroTileY());
        assertEquals(144, mosaicGrid.getNumMacroTileX());
        assertEquals(28, mosaicGrid.macroTileRegion.y);
        assertEquals(72, mosaicGrid.macroTileRegion.x);
        assertEquals(16, mosaicGrid.macroTileRegion.height);
        assertEquals(4, mosaicGrid.macroTileRegion.width);
        assertEquals(2, mosaicGrid.macroTileStepY);
        assertEquals(4, mosaicGrid.macroTileStepX);
        assertEquals(1, mosaicGrid.macroTileCountX);
        assertEquals(8, mosaicGrid.getNumReducers());
        assertEquals(4, mosaicGrid.getPartition(72,36));
        assertEquals(4, mosaicGrid.getPartition(70,36));
        assertEquals(3, mosaicGrid.getPartition(72,35));
    }

    @Test
    @Ignore
    public void testGetBounds() throws IOException {
        Product product = ProductIO.readProduct("/home/boe/tmp/sen2agri/S2A_MSIL2A_20170216T170351_N0204_R069_T14QNF_20170216T171642.SAFE/S2A_OPER_SSC_L2VALD_14QNF____20170216.HDR");
        final GeneralPath[] paths = ProductUtils.createGeoBoundaryPaths(product);
        final org.locationtech.jts.geom.Polygon[] polygons = new org.locationtech.jts.geom.Polygon[paths.length];

        for (int i = 0; i < paths.length; i++) {
            polygons[i] = convertToJtsPolygon(paths[i].getPathIterator(null));
        }
        final DouglasPeuckerSimplifier peuckerSimplifier = new DouglasPeuckerSimplifier(
                polygons.length == 1 ? polygons[0] : new GeometryFactory().createMultiPolygon(polygons));
        Geometry sourceGeometry = peuckerSimplifier.getResultGeometry();
        System.out.println("class " + sourceGeometry.getClass());
        System.out.println("sourceGeometry=" + sourceGeometry);
        Geometry regionGeometry = parseWKT("POLYGON ((-104 4, -73 4, -73 18, -104 18, -104 4))");
        Geometry effectiveGeometry = regionGeometry.intersection(sourceGeometry);
        System.out.println("class " + effectiveGeometry.getClass());
        System.out.println("effectiveGeometry=" + effectiveGeometry);
        Geometry boundary = effectiveGeometry.getBoundary();
        System.out.println("class " + boundary.getClass());
        System.out.println("boundary=" + boundary);
    }
    private org.locationtech.jts.geom.Polygon convertToJtsPolygon(PathIterator pathIterator) {
        ArrayList<double[]> coordList = new ArrayList<double[]>();
        int lastOpenIndex = 0;
        while (!pathIterator.isDone()) {
            final double[] coords = new double[6];
            final int segType = pathIterator.currentSegment(coords);
            if (segType == PathIterator.SEG_CLOSE) {
                // we should only detect a single SEG_CLOSE
                coordList.add(coordList.get(lastOpenIndex));
                lastOpenIndex = coordList.size();
            } else {
                coordList.add(coords);
            }
            pathIterator.next();
        }
        final Coordinate[] coordinates = new Coordinate[coordList.size()];
        for (int i1 = 0; i1 < coordinates.length; i1++) {
            final double[] coord = coordList.get(i1);
            coordinates[i1] = new Coordinate(coord[0], coord[1]);
        }

        GeometryFactory factory = new GeometryFactory();
        return factory.createPolygon(factory.createLinearRing(coordinates), null);
    }

}
