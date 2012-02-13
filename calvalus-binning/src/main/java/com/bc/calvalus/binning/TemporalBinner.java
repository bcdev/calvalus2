package com.bc.calvalus.binning;

import java.io.IOException;

/**
 * Processes multiple spatial bins to a single temporal bin.
 *
 * @author Norman Fomferra
 */
public class TemporalBinner {

    private final BinnerContext binnerContext;

    public TemporalBinner(BinnerContext binnerContext) {
        this.binnerContext = binnerContext;
    }

    /**
     * @return The binning context that will also be passed to {@link SpatialBinProcessor#processSpatialBinSlice(BinnerContext, java.util.List)}.
     */
    public BinnerContext getBinnerContext() {
        return binnerContext;
    }

    public TemporalBin processSpatialBins(long binIndex, Iterable<? extends SpatialBin> spatialBins) throws IOException {
        final BinManager binManager = binnerContext.getBinManager();
        TemporalBin temporalBin = binManager.createTemporalBin(binIndex);
        for (SpatialBin spatialBin : spatialBins) {
            binManager.aggregateTemporalBin(spatialBin, temporalBin);
        }
        binManager.completeTemporalBin(temporalBin);
        return temporalBin;
    }
}
