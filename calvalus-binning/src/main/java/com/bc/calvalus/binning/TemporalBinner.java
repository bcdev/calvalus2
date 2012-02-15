package com.bc.calvalus.binning;

import java.io.IOException;

/**
 * Processes multiple spatial bins to a single temporal bin.
 *
 * @author Norman Fomferra
 * @see SpatialBinner
 */
public class TemporalBinner {

    private final BinManager binManager;

    public TemporalBinner(BinningContext binningContext) {
        binManager = binningContext.getBinManager();
    }

    public TemporalBin processSpatialBins(long binIndex, Iterable<? extends SpatialBin> spatialBins) throws IOException {
        return binManager.createTemporalBin(binIndex, spatialBins);
    }
}
