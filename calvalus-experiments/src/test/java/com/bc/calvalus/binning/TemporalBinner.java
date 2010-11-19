package com.bc.calvalus.binning;

import java.util.List;

/**
 * An implementation of the {@link BinConsumer} interface that performs a temporal binning.
 */
class TemporalBinner implements BinConsumer<MyBin> {
    private final BinStore<MyBin> store;

    public TemporalBinner(BinStore<MyBin> store) {
        this.store = store;
    }

    @Override
    public void consumeSlice(int sliceIndex, List<MyBin> sliceBins) {
        for (MyBin spatialBin : sliceBins) {
            MyBin temporalBin = store.getBin(spatialBin.getIndex());
            temporalBin.addBin(spatialBin);
            store.putBin(temporalBin);
        }
    }
}
