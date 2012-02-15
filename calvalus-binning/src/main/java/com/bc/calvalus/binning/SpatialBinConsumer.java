package com.bc.calvalus.binning;

import java.util.List;

/**
 * Consumes a slice of spatial bins that are emitted by the {@link com.bc.calvalus.binning.SpatialBinner}.
 *
 * @author Norman Fomferra
 */
public interface SpatialBinConsumer {
    /**
     * Consumes an unsorted list of spatial bins.
     *
     * @param binningContext The binning context.
     * @param spatialBins    An unsorted list of spatial bins.
     * @throws Exception If any error occurs.
     */
    void consumeSpatialBins(BinningContext binningContext, List<SpatialBin> spatialBins) throws Exception;
}
