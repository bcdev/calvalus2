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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.ProductUtils;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.geom.GeneralPath;
import java.awt.geom.PathIterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the Grid on which the mosaic-ing is happening.
 *
 * @author MarcoZ
 */
public class MosaicGrid {

    private final int gridWidth;
    private final int gridHeight;
    private final int tileSize;
    private final double pixelSize;
    private final int numTileX;
    private final int numTileY;
    private GeometryFactory geometryFactory;

    public MosaicGrid() {
        this(180, 370);
    }
    public MosaicGrid(int numTileY, int tileSize) {
        this.numTileY = numTileY;
        this.numTileX = numTileY * 2;
        this.tileSize = tileSize;
        this.gridWidth = numTileX * tileSize;
        this.gridHeight = numTileY * tileSize;
        this.pixelSize = 180.0 / gridHeight;
    }

    private GeometryFactory getGeometryFactory() {
        if (geometryFactory == null) {
            geometryFactory = new GeometryFactory();
        }
        return geometryFactory;
    }


    public double getPixelSize() {
        return pixelSize;
    }

    public int getTileSize() {
        return tileSize;
    }

    public Rectangle computeRegion(Geometry roiGeometry) {
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
            final int x = (int) Math.floor((180.0 + gxmin) / pixelSize);
            final int y = (int) Math.floor((90.0 - gymax) / pixelSize);
            final int width = (int) Math.ceil((gxmax - gxmin) / pixelSize);
            final int height = (int) Math.ceil((gymax - gymin) / pixelSize);
            final Rectangle unclippedOutputRegion = new Rectangle(x, y, width, height);
            region = unclippedOutputRegion.intersection(region);
        }
        return region;
    }

    public Rectangle alignToTileGrid(Rectangle region) {
        int minX = region.x / tileSize * tileSize;
        int maxX = (region.x + region.width + tileSize - 1) / tileSize * tileSize;
        int minY = (region.y / tileSize) * tileSize;
        int maxY = (region.y + region.height + tileSize - 1) / tileSize * tileSize;

        return new Rectangle(minX, minY, maxX - minX, maxY - minY);
    }

    public Geometry getTileGeometry(int tileX, int tileY) {
        double x1 = tileXToDegree(tileX);
        double x2 = tileXToDegree(tileX + 1);
        double y1 = tileYToDegree(tileY);
        double y2 = tileYToDegree(tileY + 1);
        return getGeometryFactory().toGeometry(new Envelope(x1, x2, y1, y2));
    }

    double tileXToDegree(int tileX) {
        double degreePerTile = (double) 360 / numTileX;
        return tileX * degreePerTile - 180.0;
    }

    double tileYToDegree(int tileY) {
        double degreePerTile = (double) 180 / numTileY;
        return 90 - tileY * degreePerTile;
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

    public Point[] getTileIndices(Geometry geometry) {
        if (geometry == null) {
            Point[] points = new Point[numTileX * numTileY];

            int index = 0;
            for (int y = 0; y < numTileY; y++) {
                for (int x = 0; x < numTileX; x++) {
                    points[index++] = new Point(x, y);
                }
            }
            return points;
        } else {
            Rectangle geometryRect = computeRegion(geometry);
            Rectangle gridRect = alignToTileGrid(geometryRect);
            final int xStart = gridRect.x / tileSize;
            final int yStart = gridRect.y / tileSize;
            final int width = gridRect.width / tileSize;
            final int height = gridRect.height / tileSize;
            List<Point> points = new ArrayList<Point>(width * height);

            for (int y = yStart; y < yStart + height; y++) {
                for (int x = xStart; x < xStart + width; x++) {
                    Geometry tileGeometry = getTileGeometry(x, y);
                    Geometry intersection = geometry.intersection(tileGeometry);
                    if (!intersection.isEmpty() && intersection.getDimension() == 2) {
                        points.add(new Point(x, y));
                    }
                }
            }
            return points.toArray(new Point[points.size()]);
        }
    }
}
