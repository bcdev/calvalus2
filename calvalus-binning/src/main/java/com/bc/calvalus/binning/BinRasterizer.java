package com.bc.calvalus.binning;

/**
 * Processes temporal bins to a rectangular output raster. Used by {@link BinReprojector}.
 * <p/>
 * The processor assumes that an output raster is generated
 * with the following size:
 * <pre>
 *     BinningGrid binningGrid = ctx.getBinningGrid();
 *     int rasterWidth = binningGrid.getNumRows() * 2;
 *     int rasterHeight = binningGrid.getNumRows();
 * </pre>
 *
 * @author Norman Fomferra
 */
public abstract class BinRasterizer {
    /**
     * Called before the processing of bins begins.
     *
     * @param ctx The binning context.
     * @throws Exception If a problem occurred.
     */
    @SuppressWarnings({"UnusedParameters"})
    public void begin(BinnerContext ctx) throws Exception {
    }

    /**
     * Called after the processing of bins ends.
     *
     * @param ctx The binning context.
     * @throws Exception If a problem occurred.
     */
    public void end(BinnerContext ctx) throws Exception {
    }

    /**
     * Processes a temporal bin and its statistical output features.
     *
     * @param x            current pixel X coordinate
     * @param y            current pixel Y coordinate
     * @param temporalBin  the current temporal bin
     * @param outputVector the current output vector
     * @throws Exception if an error occurred
     */
    public abstract void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception;

    /**
     * Processes a missing bin.
     *
     * @param x current pixel X coordinate
     * @param y current pixel Y coordinate
     * @throws Exception if an error occurred
     */
    public abstract void processMissingBin(int x, int y) throws Exception;

}
