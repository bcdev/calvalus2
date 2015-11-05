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

import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.AbstractProductWriter;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ProductData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A product writer that write after all tiles forming a line are available.
 *
 */
class BufferedProductWriter extends AbstractProductWriter {

    private final ProductWriter productWriter;
    private final Map<Band, List<TileData>> buffer;

    BufferedProductWriter(ProductWriter writer) {
        super(writer.getWriterPlugIn());
        productWriter = writer;
        buffer = new HashMap<Band, List<TileData>>();
    }

    @Override
    protected void writeProductNodesImpl() throws IOException {
        productWriter.writeProductNodes(getSourceProduct(), getOutput());
    }

    @Override
    public void writeBandRasterData(Band band, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) throws IOException {
        TileData tileData = new TileData(sourceOffsetX, sourceOffsetY, sourceWidth, sourceHeight, sourceBuffer);
        List<TileData> tileDatas = buffer.get(band);
        if (tileDatas == null) {
            tileDatas = new ArrayList<TileData>();
            buffer.put(band, tileDatas);
        }
        tileDatas.add(tileData);
        mayBeWrite(band);
    }

    private void writeAll() throws IOException {
        Set<Band> bands = buffer.keySet();
        for (Band band : bands) {
            List<TileData> bandBuffer = buffer.get(band);
            if (!bandBuffer.isEmpty()) {
                write(band, bandBuffer);
                bandBuffer.clear();
            }
        }
    }

    private void mayBeWrite(Band band) throws IOException {
        List<TileData> bandBuffer = buffer.get(band);
        int size = bandBuffer.size();
        if (size > 2) {
            TileData first = bandBuffer.get(0);
            TileData last = bandBuffer.get(size - 1);
            if (first.sourceOffsetY != last.sourceOffsetY) {
                bandBuffer.remove(last);
                write(band, bandBuffer);
                bandBuffer.clear();
                bandBuffer.add(last);
            }
        }
    }

    private void write(Band band, List<TileData> tileDatas) throws IOException {
        int sceneWidth = getSourceProduct().getSceneRasterWidth();
        TileData tileDataZero = tileDatas.get(0);
        int height = tileDataZero.sourceHeight;
        int offsetY = tileDataZero.sourceOffsetY;

        ProductData line = ProductData.createInstance(tileDataZero.data.getType(), sceneWidth);
        for (int y = offsetY; y < offsetY + height; y++) {
            for (TileData tileData : tileDatas) {
                int width = tileData.sourceWidth;
                int srcPos = (y - offsetY) * width;
                int targetPos = tileData.sourceOffsetX;
                System.arraycopy(tileData.data.getElems(), srcPos, line.getElems(), targetPos, width);
            }
            productWriter.writeBandRasterData(band, 0, y, sceneWidth, 1, line, ProgressMonitor.NULL);
        }
    }

    @Override
    public void flush() throws IOException {
        writeAll();
        productWriter.flush();
    }

    @Override
    public void close() throws IOException {
        writeAll();
        productWriter.close();
    }

    @Override
    public void deleteOutput() throws IOException {
        productWriter.deleteOutput();
    }

    private static class TileData {
        final int sourceOffsetX;
        final int sourceOffsetY;
        final int sourceWidth;
        final int sourceHeight;
        final ProductData data;

        private TileData(int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData data) {
            this.sourceOffsetX = sourceOffsetX;
            this.sourceOffsetY = sourceOffsetY;
            this.sourceWidth = sourceWidth;
            this.sourceHeight = sourceHeight;
            this.data = data;
        }
    }
}
