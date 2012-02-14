package com.bc.calvalus.binning;

import java.io.IOException;

/**
 * Processes multiple spatial bins to a single temporal bin.
 *
 * @author Norman Fomferra
 */
public class TemporalBinner {

    private final BinningContext binningContext;

    public TemporalBinner(BinningContext binningContext) {
        this.binningContext = binningContext;
    }

    /**
     * @return The binning context that will also be passed to {@link SpatialBinProcessor#processSpatialBinSlice(BinningContext, java.util.List)}.
     */
    public BinningContext getBinningContext() {
        return binningContext;
    }

    public TemporalBin processSpatialBins(long binIndex, Iterable<? extends SpatialBin> spatialBins) throws IOException {
        final BinManager binManager = binningContext.getBinManager();
        return createTemporalBin(binManager, binIndex, spatialBins);
    }

    public static TemporalBin createTemporalBin(BinManager binManager, long binIndex, Iterable<? extends SpatialBin> spatialBins) {
        TemporalBin temporalBin = binManager.createTemporalBin(binIndex);
        for (SpatialBin spatialBin : spatialBins) {
            binManager.aggregateTemporalBin(spatialBin, temporalBin);
        }
        binManager.completeTemporalBin(temporalBin);
        return temporalBin;
    }
}
