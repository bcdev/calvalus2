package com.bc.calvalus.binning;

import com.bc.ceres.core.Assert;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Used to re-project temporal bins onto a rectangular grid.
 *
 * @author MarcoZ
 * @author Norman
 */
public class TemporalBinReprojector {

    private final BinningContext binningContext;
    private final TemporalBinRasterizer temporalBinRasterizer;
    private final Rectangle pixelRegion;
    private int yGlobalUltimate;

    public TemporalBinReprojector(BinningContext binningContext, TemporalBinRasterizer temporalBinRasterizer, Rectangle pixelRegion) {
        Assert.notNull(binningContext, "binningContext");
        Assert.notNull(temporalBinRasterizer, "temporalBinProcessor");
        Assert.notNull(pixelRegion, "pixelRegion");
        this.binningContext = binningContext;
        this.temporalBinRasterizer = temporalBinRasterizer;
        this.pixelRegion = pixelRegion;
    }

    public void begin() throws Exception {
        yGlobalUltimate = pixelRegion.y - 1;
        temporalBinRasterizer.begin(binningContext);
    }

    public void end() throws Exception {
        final int x1 = pixelRegion.x;
        final int x2 = x1 + pixelRegion.width - 1;
        final int y1 = pixelRegion.y;
        final int y2 = y1 + pixelRegion.height - 1;
        processRowsWithoutBins(x1, x2, yGlobalUltimate + 1, y2);
        temporalBinRasterizer.end(binningContext);
    }

    public void processBins(Iterator<? extends TemporalBin> temporalBins) throws Exception {
        final int x1 = pixelRegion.x;
        final int x2 = x1 + pixelRegion.width - 1;
        final int y1 = pixelRegion.y;
        final int y2 = y1 + pixelRegion.height - 1;
        final BinningGrid binningGrid = binningContext.getBinningGrid();
        final int gridWidth = binningGrid.getNumRows() * 2;
        final int gridHeight = binningGrid.getNumRows();

        final List<TemporalBin> binRow = new ArrayList<TemporalBin>();
        int yUltimate = -1;
        while (temporalBins.hasNext()) {
            TemporalBin temporalBin = temporalBins.next();
            long temporalBinIndex = temporalBin.getIndex();
            int y = binningGrid.getRowIndex(temporalBinIndex);
            if (y != yUltimate) {
                if (yUltimate >= y1 && yUltimate <= y2) {
                    processRowsWithoutBins(x1, x2, yGlobalUltimate + 1, yUltimate - 1);
                    processRowWithBins(yUltimate, binRow, gridWidth, gridHeight);
                    yGlobalUltimate = yUltimate;
                }
                binRow.clear();
                yUltimate = y;
            }
            binRow.add(temporalBin);
        }

        if (yUltimate >= y1 && yUltimate <= y2) {
            // last row
            processRowsWithoutBins(x1, x2, yGlobalUltimate + 1, yUltimate - 1);
            processRowWithBins(yUltimate, binRow, gridWidth, gridHeight);
            yGlobalUltimate = yUltimate;
        }
    }

    private void processRowWithBins(int y,
                                   List<TemporalBin> binRow,
                                   int gridWidth,
                                   int gridHeight) throws Exception {

        Assert.argument(!binRow.isEmpty(), "!binRow.isEmpty()");

        final int x1 = pixelRegion.x;
        final int x2 = pixelRegion.x + pixelRegion.width - 1;
        final int y1 = pixelRegion.y;
        final BinningGrid binningGrid = binningContext.getBinningGrid();
        final BinManager binManager = binningContext.getBinManager();
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
                temporalBinRasterizer.processBin(x - x1, y - y1, temporalBin, outputVector);
            } else {
                temporalBinRasterizer.processMissingBin(x - x1, y - y1);
            }
        }
    }

    private void processRowsWithoutBins(int x1, int x2, int yStart, int yEnd) throws Exception {
        for (int y = yStart; y <= yEnd; y++) {
            processRowWithoutBins(x1, x2, y - pixelRegion.y);
        }
    }

    private void processRowWithoutBins(int x1, int x2, int y) throws Exception {
        for (int x = x1; x <= x2; x++) {
            temporalBinRasterizer.processMissingBin(x - x1, y);
        }
    }
}
