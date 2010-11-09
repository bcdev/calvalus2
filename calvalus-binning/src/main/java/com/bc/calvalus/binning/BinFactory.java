package com.bc.calvalus.binning;

/**
 * A factory for special bin types.
 * @param <BIN> The bin specialisation.
 */
public interface BinFactory<BIN extends Bin> {
    // Class<BIN> getBinType();
    BIN createBin(int binIndex);
}
