package com.bc.calvalus.binning;

import com.bc.ceres.core.Assert;

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

    private final BinningContext binningContext;
    private final TemporalBinProcessor temporalBinProcessor;
    private final Rectangle pixelRegion;
    private static int yGlobalUltimate;

    public TemporalBinReprojector(BinningContext binningContext, TemporalBinProcessor temporalBinProcessor, Rectangle pixelRegion) {
        Assert.notNull(binningContext, "binningContext");
        Assert.notNull(temporalBinProcessor, "temporalBinProcessor");
        Assert.notNull(pixelRegion, "pixelRegion");
        this.binningContext = binningContext;
        this.temporalBinProcessor = temporalBinProcessor;
        this.pixelRegion = pixelRegion;
    }

    public void processBins(Iterator<TemporalBin> temporalBins) throws Exception {

        reprojectRegion(binningContext, pixelRegion, temporalBins, temporalBinProcessor);
    }

    public void begin() throws Exception {
        yGlobalUltimate = pixelRegion.y - 1;
        temporalBinProcessor.begin(binningContext);
    }

    public void end() throws Exception {
        temporalBinProcessor.end(binningContext);
        final int x1 = pixelRegion.x;
        final int x2 = x1 + pixelRegion.width - 1;
        final int y1 = pixelRegion.y;
        final int y2 = y1 + pixelRegion.height - 1;
        processRowsWithoutBins(temporalBinProcessor, x1, x2, y1, yGlobalUltimate + 1, y2);
    }

    static void reprojectRegion(BinningContext binningContext,
                                Rectangle pixelRegion,
                                Iterator<TemporalBin> temporalBins,
                                TemporalBinProcessor temporalBinProcessor) throws Exception {
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
                    processRowsWithoutBins(temporalBinProcessor, x1, x2, y1, yGlobalUltimate + 1, yUltimate - 1);
                    processRowWithBins(binningContext,
                                       pixelRegion, yUltimate, binRow,
                                       temporalBinProcessor,
                                       gridWidth, gridHeight);
                    yGlobalUltimate = yUltimate;
                }
                binRow.clear();
                yUltimate = y;
            }
            binRow.add(temporalBin);
        }

        if (yUltimate >= y1 && yUltimate <= y2) {
            // last row
            processRowsWithoutBins(temporalBinProcessor, x1, x2, y1, yGlobalUltimate + 1, yUltimate - 1);
            processRowWithBins(binningContext,
                               pixelRegion, yUltimate, binRow,
                               temporalBinProcessor,
                               gridWidth, gridHeight);
            yGlobalUltimate = yUltimate;
        }
    }

    static void processRowWithBins(BinningContext ctx,
                                   Rectangle pixelRegion,
                                   int y,
                                   List<TemporalBin> binRow,
                                   TemporalBinProcessor temporalBinProcessor,
                                   int gridWidth,
                                   int gridHeight) throws Exception {

        Assert.argument(!binRow.isEmpty(), "!binRow.isEmpty()");

        final int x1 = pixelRegion.x;
        final int x2 = pixelRegion.x + pixelRegion.width - 1;
        final int y1 = pixelRegion.y;
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

    private static void processRowsWithoutBins(TemporalBinProcessor temporalBinProcessor, int x1, int x2, int y1, int yStart, int yEnd) throws Exception {
        for (int y = yStart; y <= yEnd; y++) {
            processRowWithoutBins(temporalBinProcessor, x1, x2, y1, y);
        }
    }

    private static void processRowWithoutBins(TemporalBinProcessor temporalBinProcessor, int x1, int x2, int y1, int y) throws Exception {
        for (int x = x1; x <= x2; x++) {
            temporalBinProcessor.processMissingBin(x - x1, y - y1);
        }
    }
}
