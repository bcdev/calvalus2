/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.geom.GeneralPath;
import java.awt.geom.PathIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Defines the Grid on which the mosaic-ing is happening.
 *
 * @author MarcoZ
 */
public class MosaicGrid {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final int gridWidth;
    private final int gridHeight;
    private final int tileSize;
    private final boolean withIntersectionCheck;
    private final double pixelSize;
    private final int numTileX;
    private final int macroTileSize;
    private final int numTileY;
    private GeometryFactory geometryFactory;

    Rectangle macroTileRegion;
    int macroTileStepY;
    int macroTileStepX;
    int macroTileCountX;

    public static MosaicGrid create(Configuration conf) {
        int macroTileSize = conf.getInt("calvalus.mosaic.macroTileSize", 5);
        int numTileY = conf.getInt("calvalus.mosaic.numTileY", 180);
        int tileSize = conf.getInt("calvalus.mosaic.tileSize", 360);  // for a 300m grid
        boolean withIntersectionCheck = conf.getBoolean("calvalus.mosaic.withIntersectionCheck", true);
        int maxReducers = conf.getInt("calvalus.mosaic.maxReducers", 16);
        int maxMacroTileCountX = conf.getInt("calvalus.mosaic.numXPartitions", 0);
        String regionWkt = conf.get("calvalus.mosaic.regionGeometry");
        return new MosaicGrid(macroTileSize, numTileY, tileSize, withIntersectionCheck,maxReducers, maxMacroTileCountX, regionWkt);
    }

    MosaicGrid() {
        this(5, 180, 370, true, 16, 1, null);
    }

    MosaicGrid(int macroTileSize, int numTileY, int tileSize, boolean withIntersectionCheck, int maxReducers, int maxMacroTileCountX, String regionWkt) {
        this.macroTileSize = macroTileSize;
        this.numTileY = numTileY;
        this.numTileX = numTileY * 2;
        this.tileSize = tileSize;
        this.withIntersectionCheck = withIntersectionCheck;
        this.gridWidth = numTileX * tileSize;
        this.gridHeight = numTileY * tileSize;
        this.pixelSize = 180.0 / gridHeight;
        if (maxMacroTileCountX == 0) {
            // old behaviour
            this.macroTileRegion = new Rectangle(0, 0, numTileX/macroTileSize, numTileY/macroTileSize);
            this.macroTileStepY = 1;
            this.macroTileStepX = numTileX;
            this.macroTileCountX = 1;
        } else {
            if (regionWkt != null) {
                Geometry regionGeometry = GeometryUtils.createGeometry(regionWkt);
                Rectangle geometryBounds = computeBounds(regionGeometry);
                this.macroTileRegion = macroTileRectangleOf(geometryBounds);
            } else {
                this.macroTileRegion = new Rectangle(0, 0, numTileX / macroTileSize, numTileY / macroTileSize);
            }
            int numMacroTilesY = (int) macroTileRegion.getHeight();
            int numMacroTilesX = (int) macroTileRegion.getWidth();
            if (numMacroTilesY * numMacroTilesX <= maxReducers && maxMacroTileCountX > 1) {
                macroTileStepY = 1;
                macroTileStepX = 1;
                macroTileCountX = numMacroTilesX;
                System.out.println("case full, ty="+numMacroTilesY+" tx="+numMacroTilesX+" mr="+maxReducers+" dy="+macroTileStepY+" dx="+macroTileStepX+" nx="+macroTileCountX);
            } else if (numMacroTilesY * 2 <= maxReducers && maxMacroTileCountX > 1) {
                macroTileStepY = 1;
                macroTileStepX = (numMacroTilesX + (maxReducers / numMacroTilesY) - 1) / (maxReducers / numMacroTilesY);
                macroTileCountX = (numMacroTilesX + macroTileStepX - 1) / macroTileStepX;
                System.out.println("case partx, ty="+numMacroTilesY+" tx="+numMacroTilesX+" mr="+maxReducers+" dy="+macroTileStepY+" dx="+macroTileStepX+" nx="+macroTileCountX);
            } else if (numMacroTilesY <= maxReducers) {
                macroTileStepY = 1;
                macroTileStepX = numMacroTilesX;
                macroTileCountX = 1;
                System.out.println("case fully, ty="+numMacroTilesY+" tx="+numMacroTilesX+" mr="+maxReducers+" dy="+macroTileStepY+" dx="+macroTileStepX+" nx="+macroTileCountX);
            } else {
                macroTileStepY = (numMacroTilesY + maxReducers - 1) / maxReducers;
                macroTileStepX = numMacroTilesX;
                macroTileCountX = 1;
                System.out.println("case party, ty="+numMacroTilesY+" tx="+numMacroTilesX+" mr="+maxReducers+" dy="+macroTileStepY+" dx="+macroTileStepX+" nx="+macroTileCountX);
            }
        }
    }

    public void saveToConfiguration(Configuration conf) {
        conf.setInt("calvalus.mosaic.macroTileSize", macroTileSize);
        conf.setInt("calvalus.mosaic.numTileY", numTileY);
        conf.setInt("calvalus.mosaic.tileSize", tileSize);
        conf.setBoolean("calvalus.mosaic.withIntersectionCheck", withIntersectionCheck);
    }

    private GeometryFactory getGeometryFactory() {
        if (geometryFactory == null) {
            geometryFactory = new GeometryFactory();
        }
        return geometryFactory;
    }

    public int getMacroTileSize() {
        return macroTileSize;
    }

    public int getTileSize() {
        return tileSize;
    }

    public double getPixelSize() {
        return pixelSize;
    }

    public int getNumTileX() {
        return numTileX;
    }

    public int getNumTileY() {
        return numTileY;
    }

    public int getNumMacroTileX() {
        return numTileX / macroTileSize;
    }

    public int getNumMacroTileY() {
        return numTileY / macroTileSize;
    }

    public Rectangle computeBounds(Geometry roiGeometry) {
        Rectangle region = new Rectangle(gridWidth, gridHeight);
        if (roiGeometry != null) {
            final Coordinate[] coordinates = roiGeometry.getBoundary().getCoordinates();
            double gxmin = Double.POSITIVE_INFINITY;
            double gxmax = Double.NEGATIVE_INFINITY;
            double gymin = Double.POSITIVE_INFINITY;
            double gymax = Double.NEGATIVE_INFINITY;
            for (Coordinate coordinate : coordinates) {
                gxmin = Math.min(gxmin, coordinate.x);
                gxmax = Math.max(gxmax, coordinate.x);
                gymin = Math.min(gymin, coordinate.y);
                gymax = Math.max(gymax, coordinate.y);
            }
            // extend by 1/2 pixelSize in all directions
            // to circumscribe the complete extend of the source
            // (sourcePixelSize would be even more accurate)
            gxmin -= pixelSize / 2;
            gxmax += pixelSize / 2;
            gymin -= pixelSize / 2;
            gymax += pixelSize / 2;
            if (gxmin < -180.0 || gxmax > 180.0) {
                gxmin = -180.0;
                gxmax = 180.0;
            }
            if (gymin < -90.0) {
                gymin = -90.0;
            }
            if (gymax > 90.0) {
                gymax = 90.0;
            }

            final int x = (int) Math.floor((180.0 + gxmin) / pixelSize);
            final int y = (int) Math.floor((90.0 - gymax) / pixelSize);
            final int width = (int) Math.ceil((gxmax - gxmin) / pixelSize + 1);
            final int height = (int) Math.ceil((gymax - gymin) / pixelSize + 1);
            final Rectangle unclippedOutputRegion = new Rectangle(x, y, width, height);
            region = unclippedOutputRegion.intersection(region);
        }
        LOG.info("source product bounds: " + region);
        return region;
    }

    public Rectangle alignToTileGrid(Rectangle rectangle) {
        int minX = rectangle.x / tileSize * tileSize;
        int maxX = (rectangle.x + rectangle.width + tileSize - 1) / tileSize * tileSize;
        int minY = (rectangle.y / tileSize) * tileSize;
        int maxY = (rectangle.y + rectangle.height + tileSize - 1) / tileSize * tileSize;

        return new Rectangle(minX, minY, maxX - minX, maxY - minY);
    }

    Geometry getTileGeometry(int tileX, int tileY) {
        double x1 = tileXToDegree(tileX);
        double x2 = tileXToDegree(tileX + 1);
        double y1 = tileYToDegree(tileY);
        double y2 = tileYToDegree(tileY + 1);
        return getGeometryFactory().toGeometry(new Envelope(x1, x2, y1, y2));
    }

    double tileXToDegree(int tileX) {
        final double degreePerTile = (double) 360 / numTileX;
        final double lon = tileX * degreePerTile - 180.0;
        return lon;
    }

    int degreeToTileX(double lon) {
        final double degreePerTile = (double) 360 / numTileX;
        final int tileX = (int) (Math.floor(lon + 180.0) / degreePerTile);
        return tileX;
    }

    double tileYToDegree(int tileY) {
        double degreePerTile = (double) 180 / numTileY;
        double lat = 90 - tileY * degreePerTile;
        return lat;
    }

    int degreeToTileY(double lat) {
        final double degreePerTile = (double) 180 / numTileY;
        final int tileY = (int) (Math.floor((lat - 90.0) * -1 / degreePerTile));
        return tileY;
    }

    public Geometry computeProductGeometry(Product product) {
        try {
            final GeneralPath[] paths = ProductUtils.createGeoBoundaryPaths(product);
            final Polygon[] polygons = new Polygon[paths.length];

            for (int i = 0; i < paths.length; i++) {
                polygons[i] = convertToJtsPolygon(paths[i].getPathIterator(null));
            }
            final DouglasPeuckerSimplifier peuckerSimplifier = new DouglasPeuckerSimplifier(
                    polygons.length == 1 ? polygons[0] : getGeometryFactory().createMultiPolygon(polygons));
            return peuckerSimplifier.getResultGeometry();
        } catch (Exception e) {
            return null;
        }
    }

    private Polygon convertToJtsPolygon(PathIterator pathIterator) {
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

        GeometryFactory factory = getGeometryFactory();
        return factory.createPolygon(factory.createLinearRing(coordinates), null);
    }

    public TileIndexWritable[] getTileIndices(List<Point> tilePointIndices) {
        TileIndexWritable[] tileIndices = new TileIndexWritable[tilePointIndices.size()];
        for (int i = 0; i < tilePointIndices.size(); i++) {
            Point point = tilePointIndices.get(i);
            int macroX = point.x / macroTileSize;
            int macroY = point.y / macroTileSize;
            tileIndices[i] = new TileIndexWritable(macroX, macroY, point.x, point.y);
        }
        return tileIndices;
    }

    List<Point> getTilePointIndicesFromProduct(Product product, Geometry geometry) {
        GeoCoding geoCoding = product.getSceneGeoCoding();
        int width = product.getSceneRasterWidth();
        int height = product.getSceneRasterHeight();
        PixelPos pixelPos = new PixelPos();
        GeoPos geoPos = new GeoPos();

        PreparedGeometry pGeom = null;
        GeometryFactory geoFactory = null;
        if (geometry != null) {
            pGeom = PreparedGeometryFactory.prepare(geometry);
            geoFactory = getGeometryFactory();
        }
        Set<Point> pointSet = new HashSet<>();
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                pixelPos.setLocation(x + 0.5f, y + 0.5f);
                geoCoding.getGeoPos(pixelPos, geoPos);
                if (geoPos.isValid()) {
                    if (pGeom == null || pGeom.contains(geoFactory.createPoint(new Coordinate(geoPos.lon, geoPos.lat)))) {
                        int tileX = degreeToTileX(geoPos.lon);
                        int tileY = degreeToTileY(geoPos.lat);
                        pointSet.add(new Point(tileX, tileY));
                    }
                }
            }
        }
        List<Point> pointList = new ArrayList<>(pointSet);
        Collections.sort(pointList, new Comparator<Point>() {
            @Override
            public int compare(Point p1, Point p2) {
                int cmp = Integer.compare(p1.x, p2.x);
                if (cmp == 0) {
                    cmp = Integer.compare(p1.y, p2.y);
                }
                return cmp;
            }
        });
        return pointList;
    }

    List<Point> getTilePointIndicesFromGeometry(Geometry geometry) {
        Rectangle geometryBounds = computeBounds(geometry);
        Rectangle gridRect = alignToTileGrid(geometryBounds);
        final int xStart = gridRect.x / tileSize;
        final int yStart = gridRect.y / tileSize;
        final int width = gridRect.width / tileSize;
        final int height = gridRect.height / tileSize;
        List<Point> points = new ArrayList<>(width * height);

        if (withIntersectionCheck) {
            for (int y = yStart; y < yStart + height; y++) {
                for (int x = xStart; x < xStart + width; x++) {
                    Geometry tileGeometry = getTileGeometry(x, y);
                    Geometry intersection = geometry.intersection(tileGeometry);
                    if (!intersection.isEmpty() && intersection.getDimension() == 2) {
                        points.add(new Point(x, y));
                    }
                }
            }
        } else {
            for (int y = yStart; y < yStart + height; y++) {
                for (int x = xStart; x < xStart + width; x++) {
                    points.add(new Point(x, y));
                }
            }
        }
        return points;
    }

    List<Point> getTilePointIndicesGlobal() {
        List<Point> points = new ArrayList<>(numTileX * numTileY);

        for (int y = 0; y < numTileY; y++) {
            for (int x = 0; x < numTileX; x++) {
                points.add(new Point(x, y));
            }
        }
        return points;
    }

    public Rectangle getTileRectangle(int tileX, int tileY) {
        return new Rectangle(tileX * tileSize, tileY * tileSize, tileSize, tileSize);
    }

    public Rectangle getMacroTileRectangle(int tileX, int tileY) {
        int pixelPerTile = tileSize * macroTileSize;
        return new Rectangle(tileX * pixelPerTile, tileY * pixelPerTile, pixelPerTile, pixelPerTile);
    }

    public CrsGeoCoding createMacroCRS(Point macroTile) {
        CrsGeoCoding geoCoding;
        try {
            Rectangle productRegion = getMacroTileRectangle(macroTile.x, macroTile.y);
            geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84,
                                         productRegion.width,
                                         productRegion.height,
                                         -180.0 + pixelSize * productRegion.x,
                                         90.0 - pixelSize * productRegion.y,
                                         pixelSize,
                                         pixelSize,
                                         0.0, 0.0);
        } catch (FactoryException e) {
            throw new IllegalStateException(e);
        } catch (TransformException e) {
            throw new IllegalStateException(e);
        }
        return geoCoding;
    }

    private Rectangle macroTileRectangleOf(Rectangle rectangle) {
        int minX = rectangle.x / tileSize / macroTileSize;
        int maxX = (rectangle.x + rectangle.width + tileSize * macroTileSize - 1) / tileSize / macroTileSize;
        int minY = rectangle.y / tileSize / macroTileSize;
        int maxY = (rectangle.y + rectangle.height + tileSize * macroTileSize - 1) / tileSize / macroTileSize;
        return new Rectangle(minX, minY, maxX - minX, maxY - minY);
    }

    public int getPartition(int macroTileX, int macroTileY) {
        return ((macroTileY - macroTileRegion.y) / macroTileStepY) * macroTileCountX + (macroTileX - macroTileRegion.x) / macroTileStepX;
    }

    public int getNumReducers() {
        int numMacroTilesY = (int) macroTileRegion.getHeight();
        return ((numMacroTilesY + macroTileStepY - 1) / macroTileStepY) * macroTileCountX;
    }
}
