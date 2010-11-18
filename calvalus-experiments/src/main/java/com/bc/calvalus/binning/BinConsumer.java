package com.bc.calvalus.binning;

import java.util.List;

/**
 * Processes a spatial slice of bin cells.
 * Called by {@link SpatialBinner}.
 * @param <BIN>
 */
public interface BinConsumer<BIN extends Bin> {
    void consumeSlice(int sliceIndex, List<BIN> sliceBins) throws Exception;
}
