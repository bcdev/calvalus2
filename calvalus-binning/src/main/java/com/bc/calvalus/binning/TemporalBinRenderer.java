package com.bc.calvalus.binning;

import java.awt.*;
import java.io.IOException;

/**
 * Renders temporal bins to a rectangular output raster. Used by {@link Reprojector}.
 * <p/>
 * Bin renderers can render either maximum rasters of the size
 * <pre>
 *     BinningGrid binningGrid = ctx.getBinningGrid();
 *     int rasterWidth = 2 * binningGrid.getNumRows();
 *     int rasterHeight = binningGrid.getNumRows();
 * </pre>
 * or just render a sub-region returned by {@link #getRasterRegion()}.
 *
 * @author Norman Fomferra
 * @see Reprojector
 */
public interface TemporalBinRenderer {

    /**
     * @return The raster sub-region that this renderer will render the bins into.
     */
    Rectangle getRasterRegion();

    /**
     * Called once before the rendering of bins begins.
     *
     * @param context The binning context.
     * @throws IOException If an I/O error occurred.
     */
    void begin(BinningContext context) throws IOException;

    /**
     * Called once after the rendering of bins ends.
     *
     * @param context The binning context.
     * @throws IOException If an I/O error occurred.
     */
    void end(BinningContext context) throws IOException;

    /**
     * Renders a temporal bin and its statistical output features into the raster at pixel position (x,y).
     * Called for each (x,y) where there is data, with increasing X and increasing Y, X varies faster.
     *
     * @param x            The current raster pixel X coordinate
     * @param y            The current raster pixel Y coordinate
     * @param temporalBin  the current temporal bin
     * @param outputVector the current output vector
     * @throws IOException If an I/O error occurred.
     */
    void renderBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws IOException;

    /**
     * Renders a missing temporal bin.
     * Called for each (x,y) where there is no data, with increasing X and increasing Y, X varies faster.
     *
     * @param x current pixel X coordinate
     * @param y current pixel Y coordinate
     * @throws IOException If an I/O error occurred.
     */
    void renderMissingBin(int x, int y) throws IOException;
}
