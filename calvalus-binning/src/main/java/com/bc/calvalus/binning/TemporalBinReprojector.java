package com.bc.calvalus.binning;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Used to reproject temporal bins onto a rectangular grid.
 *
 * @author MarcoZ
 * @author Norman
 */
public class TemporalBinReprojector {
    public static void reprojectRow(BinningContext ctx,
                                    Rectangle pixelRegion,
                                    int y,
                                    List<TemporalBin> binRow,
                                    TemporalBinProcessor temporalBinProcessor,
                                    int gridWidth,
                                    int gridHeight) throws Exception {
        final int x1 = pixelRegion.x;
        final int x2 = pixelRegion.x + pixelRegion.width - 1;
        final int y1 = pixelRegion.y;
        if (binRow.isEmpty()) {
            for (int x = x1; x <= x2; x++) {
                temporalBinProcessor.processMissingBin(x - x1, y - y1);
            }
            return;
        }
        final BinningGrid binningGrid = ctx.getBinningGrid();
        final BinManager binManager = ctx.getBinManager();
        final WritableVector outputVector = binManager.createOutputVector();
        final double lat = 90.0 - (y + 0.5) * 180.0 / gridHeight;
        long lastBinIndex = -1;
        TemporalBin temporalBin = null;
        int rowIndex = -1;
        for (int x = x1; x <= x2; x++) {
            double lon = -180.0 + (x + 0.5) * 360.0 / gridWidth;
            long wantedBinIndex = binningGrid.getBinIndex(lat, lon);
            if (lastBinIndex != wantedBinIndex) {
                // search temporalBin for wantedBinIndex
                temporalBin = null;
                for (int i = rowIndex + 1; i < binRow.size(); i++) {
                    final long binIndex = binRow.get(i).getIndex();
                    if (binIndex == wantedBinIndex) {
                        temporalBin = binRow.get(i);
                        binManager.computeOutput(temporalBin, outputVector);
                        lastBinIndex = wantedBinIndex;
                        rowIndex = i;
                        break;
                    } else if (binIndex > wantedBinIndex) {
                        break;
                    }
                }
            }
            if (temporalBin != null) {
                temporalBinProcessor.processBin(x - x1, y - y1, temporalBin, outputVector);
            } else {
                temporalBinProcessor.processMissingBin(x - x1, y - y1);
            }
        }
    }

    public static void reprojectPart(BinningContext binningContext,
                                     Rectangle pixelRegion,
                                     Iterator<TemporalBin> temporalBins,
                                     TemporalBinProcessor temporalBinProcessor) throws Exception {
        final int y1 = pixelRegion.y;
        final int y2 = pixelRegion.y + pixelRegion.height - 1;
        final BinningGrid binningGrid = binningContext.getBinningGrid();
        final int gridWidth = binningGrid.getNumRows() * 2;
        final int gridHeight = binningGrid.getNumRows();

        int lastRowIndex = -1;
        final ArrayList<TemporalBin> binRow = new ArrayList<TemporalBin>();
        while (temporalBins.hasNext()) {
            TemporalBin temporalBin = temporalBins.next();
            int rowIndex = binningGrid.getRowIndex(temporalBin.getIndex());
            if (rowIndex != lastRowIndex) {
                if (lastRowIndex >= y1 && lastRowIndex <= y2) {
                    reprojectRow(binningContext,
                                 pixelRegion, lastRowIndex, binRow,
                                 temporalBinProcessor,
                                 gridWidth, gridHeight);
                }
                binRow.clear();
                lastRowIndex = rowIndex;
            }
            binRow.add(temporalBin);
        }

        if (lastRowIndex >= y1 && lastRowIndex <= y2) {
            // last row
            reprojectRow(binningContext,
                         pixelRegion, lastRowIndex, binRow,
                         temporalBinProcessor,
                         gridWidth, gridHeight);
        }
    }
}
