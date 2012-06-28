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

import org.junit.Before;
import org.junit.Test;

import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MosaicTileHandlerTest {

    private DummyMosaicTileHandler tileHandler;
    private MosaicGrid mosaicGrid;

    @Before
    public void setUp() throws Exception {
        mosaicGrid = new MosaicGrid(3, 9, 2);
        tileHandler = new DummyMosaicTileHandler(mosaicGrid);
    }

    @Test
    public void testCreateTileIndices() throws Exception {
        Point[] tileIndices = tileHandler.createTileIndices(mosaicGrid.getMacroTileSize());
        assertNotNull(tileIndices);
        assertEquals(9, tileIndices.length);
        assertEquals(new Point(0, 0), tileIndices[0]);
        assertEquals(new Point(1, 0), tileIndices[1]);
        assertEquals(new Point(2, 0), tileIndices[2]);
        assertEquals(new Point(0, 1), tileIndices[3]);
        assertEquals(new Point(1, 1), tileIndices[4]);
        assertEquals(new Point(2, 1), tileIndices[5]);
        assertEquals(new Point(2, 2), tileIndices[8]);

    }

    @Test
    public void testGetMissingTileIndices() throws Exception {
        Point[] missingIndices = tileHandler.getMissingTileIndices(new Point(0, 0), new Point(2, 0));
        assertNotNull(missingIndices);
        assertEquals(1, missingIndices.length);
        assertEquals(new Point(1, 0), missingIndices[0]);

        missingIndices = tileHandler.getMissingTileIndices(new Point(1, 0), new Point(2, 0));
        assertNotNull(missingIndices);
        assertEquals(0, missingIndices.length);

        missingIndices = tileHandler.getMissingTileIndices(new Point(1, 0), new Point(2, 1));
        assertNotNull(missingIndices);
        assertEquals(3, missingIndices.length);
        assertEquals(new Point(2, 0), missingIndices[0]);
        assertEquals(new Point(0, 1), missingIndices[1]);
        assertEquals(new Point(1, 1), missingIndices[2]);

        missingIndices = tileHandler.getMissingTileIndices(null, new Point(1, 1));
        assertNotNull(missingIndices);
        assertEquals(4, missingIndices.length);
        assertEquals(new Point(0, 0), missingIndices[0]);
        assertEquals(new Point(1, 0), missingIndices[1]);
        assertEquals(new Point(2, 0), missingIndices[2]);
        assertEquals(new Point(0, 1), missingIndices[3]);

        missingIndices = tileHandler.getMissingTileIndices(new Point(0, 2), null);
        assertNotNull(missingIndices);
        assertEquals(2, missingIndices.length);
        assertEquals(new Point(1, 2), missingIndices[0]);
        assertEquals(new Point(2, 2), missingIndices[1]);
    }

    @Test
    public void testGetCurrentMacroTile() throws Exception {
        assertNull(tileHandler.getCurrentMacroTile());
        tileHandler.handleTile(createKey(0, 0, 2, 3), null);
        assertNotNull(tileHandler.getCurrentMacroTile());
        assertEquals(new Point(0, 0), tileHandler.getCurrentMacroTile());
        tileHandler.handleTile(createKey(0, 0, 4, 3), null);
        assertEquals(new Point(0, 0), tileHandler.getCurrentMacroTile());
        tileHandler.handleTile(createKey(0, 2, 4, 3), null);
        assertEquals(new Point(0, 2), tileHandler.getCurrentMacroTile());
    }

    @Test
    public void testGetRelativeTileIndex() throws Exception {
        assertEquals(new Point(0, 0), tileHandler.getRelativeTileIndex(createKey(0, 0, 0, 0)));
        assertEquals(new Point(1, 0), tileHandler.getRelativeTileIndex(createKey(0, 0, 1, 0)));
        assertEquals(new Point(2, 1), tileHandler.getRelativeTileIndex(createKey(0, 0, 2, 1)));

        assertEquals(new Point(0, 0), tileHandler.getRelativeTileIndex(createKey(1, 2, 0, 0)));

        assertEquals(new Point(0, 0), tileHandler.getRelativeTileIndex(createKey(2, 1, 0, 0)));
        assertEquals(new Point(1, 0), tileHandler.getRelativeTileIndex(createKey(2, 1, 1, 0)));
        assertEquals(new Point(2, 1), tileHandler.getRelativeTileIndex(createKey(2, 1, 2, 1)));

        assertEquals(new Point(0, 0), tileHandler.getRelativeTileIndex(createKey(4, 10, 0, 0)));
        assertEquals(new Point(1, 0), tileHandler.getRelativeTileIndex(createKey(4, 10, 1, 0)));
        assertEquals(new Point(2, 1), tileHandler.getRelativeTileIndex(createKey(4, 10, 2, 1)));
    }

    @Test
    public void testProductWriting_AllTilesPresent_MacroTile_0_0() throws Exception {
        tileHandler = new DummyMosaicTileHandler(new MosaicGrid(3, 9, 2));
        assertNull(tileHandler.createProduct);
        assertEquals(0, tileHandler.closeProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(0, tileHandler.dataTiles.size());

        tileHandler.handleTile(createKey(0, 0, 0, 0), new TileDataWritable(new float[][]{}));
        assertEquals(new Point(0, 0), tileHandler.createProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(1, tileHandler.dataTiles.size());
        assertEquals(new Point(0, 0), tileHandler.dataTiles.get(0));
        tileHandler.handleTile(createKey(0, 0, 1, 0), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(0, 0, 2, 0), new TileDataWritable(new float[][]{}));

        tileHandler.handleTile(createKey(0, 0, 0, 1), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(0, 0, 1, 1), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(0, 0, 2, 1), new TileDataWritable(new float[][]{}));

        tileHandler.handleTile(createKey(0, 0, 0, 2), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(0, 0, 1, 2), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(0, 0, 2, 2), new TileDataWritable(new float[][]{}));
        assertEquals(0, tileHandler.closeProduct);
        tileHandler.close();

        assertEquals(new Point(0, 0), tileHandler.createProduct);
        assertEquals(1, tileHandler.closeProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(9, tileHandler.dataTiles.size());
        assertEquals(new Point(0, 0), tileHandler.dataTiles.get(0));
    }

    @Test
    public void testProductWriting_SomeTilesPresent_2_MacroTiles() throws Exception {
        tileHandler = new DummyMosaicTileHandler(new MosaicGrid(3, 9, 2));
        assertNull(tileHandler.createProduct);
        assertEquals(0, tileHandler.closeProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(0, tileHandler.dataTiles.size());

        tileHandler.handleTile(createKey(0, 0, 1, 0), new TileDataWritable(new float[][]{}));
        assertEquals(new Point(0, 0), tileHandler.createProduct);
        assertEquals(1, tileHandler.nanTiles.size());
        assertEquals(1, tileHandler.dataTiles.size());
        assertEquals(new Point(1, 0), tileHandler.dataTiles.get(0));

        tileHandler.handleTile(createKey(0, 0, 2, 0), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(0, 0, 2, 1), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(0, 0, 0, 2), new TileDataWritable(new float[][]{}));

        assertEquals(0, tileHandler.closeProduct);

        tileHandler.handleTile(createKey(1, 0, 1, 1), new TileDataWritable(new float[][]{}));

        assertEquals(new Point(1, 0), tileHandler.createProduct);
        assertEquals(1, tileHandler.closeProduct);
        assertEquals(5 + 4, tileHandler.nanTiles.size());
        assertEquals(4 + 1, tileHandler.dataTiles.size());
        assertEquals(new Point(1, 0), tileHandler.dataTiles.get(0));

        tileHandler.close();

        assertEquals(new Point(1, 0), tileHandler.createProduct);
        assertEquals(2, tileHandler.closeProduct);
        assertEquals(5 + 8, tileHandler.nanTiles.size());
        assertEquals(4 + 1, tileHandler.dataTiles.size());
        assertEquals(new Point(1, 0), tileHandler.dataTiles.get(0));
    }

    @Test
    public void testProductWriting_NoTilesPresent_MacroTile_0_0() throws Exception {
        tileHandler = new DummyMosaicTileHandler(new MosaicGrid(3, 9, 2));
        assertNull(tileHandler.createProduct);
        assertEquals(0, tileHandler.closeProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(0, tileHandler.dataTiles.size());

        tileHandler.close();

        assertNull(tileHandler.createProduct);
        assertEquals(0, tileHandler.closeProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(0, tileHandler.dataTiles.size());
    }

    @Test
    public void testProductWriting_AllTilesPresent_MacroTile_1_2() throws Exception {
        tileHandler = new DummyMosaicTileHandler(new MosaicGrid(3, 9, 2));
        assertNull(tileHandler.createProduct);
        assertEquals(0, tileHandler.closeProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(0, tileHandler.dataTiles.size());

        tileHandler.handleTile(createKey(1, 2, 0, 0), new TileDataWritable(new float[][]{}));
        assertEquals(new Point(1, 2), tileHandler.createProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(1, tileHandler.dataTiles.size());
        assertEquals(new Point(0, 0), tileHandler.dataTiles.get(0));
        tileHandler.handleTile(createKey(1, 2, 1, 0), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(1, 2, 2, 0), new TileDataWritable(new float[][]{}));

        tileHandler.handleTile(createKey(1, 2, 0, 1), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(1, 2, 1, 1), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(1, 2, 2, 1), new TileDataWritable(new float[][]{}));

        tileHandler.handleTile(createKey(1, 2, 0, 2), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(1, 2, 1, 2), new TileDataWritable(new float[][]{}));
        tileHandler.handleTile(createKey(1, 2, 2, 2), new TileDataWritable(new float[][]{}));
        assertEquals(0, tileHandler.closeProduct);
        tileHandler.close();

        assertEquals(new Point(1, 2), tileHandler.createProduct);
        assertEquals(1, tileHandler.closeProduct);
        assertEquals(0, tileHandler.nanTiles.size());
        assertEquals(9, tileHandler.dataTiles.size());
        assertEquals(new Point(0, 0), tileHandler.dataTiles.get(0));
    }

    private TileIndexWritable createKey(int macroTileX, int macroTileY, int tileX, int tileY) {
        int tileXAbsolut = mosaicGrid.getMacroTileSize() * macroTileX + tileX;
        int tileYAbsolut = mosaicGrid.getMacroTileSize() * macroTileY + tileY;
        return new TileIndexWritable(macroTileX, macroTileY, tileXAbsolut, tileYAbsolut);
    }

    private class DummyMosaicTileHandler extends MosaicTileHandler {

        private Point createProduct = null;
        private int closeProduct = 0;
        private List<Point> nanTiles = new ArrayList<Point>();
        private List<Point> dataTiles = new ArrayList<Point>();

        private DummyMosaicTileHandler(MosaicGrid mosaicGrid) {
            super(mosaicGrid);
        }

        @Override
        protected void createProduct(Point macroTile) {
            createProduct = macroTile;
        }

        @Override
        protected void writeDataTile(Point tile, TileDataWritable data) {
            dataTiles.add(tile);
        }

        @Override
        protected void writeNaNTile(Point tile) throws IOException {
            nanTiles.add(tile);
        }

        @Override
        protected void finishProduct() {
            closeProduct++;
        }

    }
}
