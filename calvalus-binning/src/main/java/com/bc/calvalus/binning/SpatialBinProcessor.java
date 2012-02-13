package com.bc.calvalus.binning;

import java.util.List;

/**
 * Processes a slice of spatial bins.
 * Called by {@link com.bc.calvalus.binning.SpatialBinner}.
 *
 * @author Norman Fomferra
 */
public interface SpatialBinProcessor {
    /**
     * Processes a slice of spatial bins.
     *
     * @param ctx         The binning context.
     * @param spatialBins The slice of spatial bins.
     * @throws Exception If an error occurs.
     */
    void processSpatialBinSlice(BinnerContext ctx, List<SpatialBin> spatialBins) throws Exception;
}
