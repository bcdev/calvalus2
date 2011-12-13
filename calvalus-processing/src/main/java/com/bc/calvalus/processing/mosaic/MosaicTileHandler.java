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

import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes a series of products base on the tile data it receives.
 *
 * @author MarcoZ
 */
abstract class MosaicTileHandler {

    private final MosaicGrid mosaicGrid;
    private final Point[] indices;

    private Point currentMacroTile = null;
    private Point currentTile = null;

    public MosaicTileHandler(MosaicGrid mosaicGrid) {
        this.mosaicGrid = mosaicGrid;
        this.indices = createTileIndices(mosaicGrid.getMacroTileSize());
    }

    public void handleTile(TileIndexWritable key, TileDataWritable data) throws IOException {
        Point tile = getRelativeTileIndex(key);
        if (currentMacroTile == null ||
                key.getMacroTileY() != currentMacroTile.y ||
                key.getMacroTileX() != currentMacroTile.x) {
            Point macroTile = new Point(key.getMacroTileX(), key.getMacroTileY());
            if (currentMacroTile != null) {
                close();
            }
            currentMacroTile = macroTile;
            createProduct(currentMacroTile);
        }
        writeNaNTiles(currentTile, tile);
        currentTile = tile;
        writeDataTile(tile, data);
    }

    public void close() throws IOException {
        if (currentMacroTile != null) {
            writeNaNTiles(currentTile, null);
            closeProduct();
        }
        currentTile = null;
        currentMacroTile = null;
    }
    private void writeNaNTiles(Point start, Point stop) throws IOException {
        Point[] missingTileIndices = getMissingTileIndices(start, stop);
        for (Point tile : missingTileIndices) {
            writeNaNTile(tile);
        }
    }

    Point getRelativeTileIndex(TileIndexWritable key) {
        int tileX = key.getTileX() % mosaicGrid.getMacroTileSize();
        int tileY = key.getTileY() % mosaicGrid.getMacroTileSize();
        return new Point(tileX, tileY);
    }

    Point getCurrentMacroTile() {
        return currentMacroTile;
    }

    MosaicGrid getMosaicGrid() {
        return mosaicGrid;
    }

    Point[] getMissingTileIndices(Point start, Point stop) {
        List<Point> missing = new ArrayList<Point>(indices.length);
        boolean insert = (start == null);
        for (Point index : indices) {
            if (index.equals(stop)) {
                break;
            } else if (index.equals(start)) {
                insert = true;
            } else if (insert) {
                missing.add(index);
            }
        }
        return missing.toArray(new Point[missing.size()]);
    }

    static Point[] createTileIndices(int macroTileSize) {
        Point[] indices = new Point[macroTileSize * macroTileSize];
        int index = 0;
        for (int y = 0; y < macroTileSize; y++) {
            for (int x = 0; x < macroTileSize; x++) {
                indices[index++] = new Point(x, y);
            }
        }
        return indices;
    }

    protected abstract void createProduct(Point macroTile) throws IOException;

    protected abstract void writeDataTile(Point tile, TileDataWritable data) throws IOException;

    protected abstract void writeNaNTile(Point tile) throws IOException;

    protected abstract void closeProduct() throws IOException;

}
