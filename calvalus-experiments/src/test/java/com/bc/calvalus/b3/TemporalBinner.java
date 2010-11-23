package com.bc.calvalus.b3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of the {@link com.bc.calvalus.binning.SpatialBinProcessor} interface that performs a temporal binning.
 */
class TemporalBinner implements SpatialBinProcessor {
    private final BinManager binManager;
    final Map<Integer, TemporalBin> binMap ;

    TemporalBinner(BinManager binManager) {
        this.binManager = binManager;
        this.binMap = new HashMap<Integer, TemporalBin>();
    }

    @Override
    public void processSpatialBinSlice(BinningContext ctx, int sliceIndex, List<SpatialBin> sliceBins) {
        for (SpatialBin spatialBin : sliceBins) {
            TemporalBin temporalBin = binMap.get(spatialBin.getIndex());
            if (temporalBin == null) {
                temporalBin = binManager.createTemporalBin(spatialBin.getIndex());
            }
            ctx.getBinManager().aggregateTemporalBin(spatialBin, temporalBin);
            binMap.put(temporalBin.getIndex(), temporalBin);
        }
    }
}
