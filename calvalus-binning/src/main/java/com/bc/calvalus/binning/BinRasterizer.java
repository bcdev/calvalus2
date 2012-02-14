package com.bc.calvalus.binning;

import java.io.IOException;

/**
 * Processes temporal bins to a rectangular output raster. Used by {@link BinReprojector}.
 * <p/>
 * The processor assumes that an output raster is generated
 * with the following size:
 * <pre>
 *     BinningGrid binningGrid = ctx.getBinningGrid();
 *     int rasterWidth = 2 * binningGrid.getNumRows();
 *     int rasterHeight = binningGrid.getNumRows();
 * </pre>
 *
 * @author Norman Fomferra
 */
public interface BinRasterizer {

    /**
     * Called before the processing of bins begins.
     *
     * @param context The binning context.
     * @throws IOException If an I/O error occurred.
     */
    void begin(BinningContext context) throws IOException;

    /**
     * Called after the processing of bins ends.
     *
     * @param context The binning context.
     * @throws IOException If an I/O error occurred.
     */
    void end(BinningContext context) throws IOException;

    /**
     * Processes a temporal bin and its statistical output features.
     *
     * @param x            current pixel X coordinate
     * @param y            current pixel Y coordinate
     * @param temporalBin  the current temporal bin
     * @param outputVector the current output vector
     * @throws IOException If an I/O error occurred.
     */
    void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws IOException;

    /**
     * Processes a missing bin.
     *
     * @param x current pixel X coordinate
     * @param y current pixel Y coordinate
     * @throws IOException If an I/O error occurred.
     */
    void processMissingBin(int x, int y) throws IOException;
}
