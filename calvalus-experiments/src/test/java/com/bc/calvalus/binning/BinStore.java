package com.bc.calvalus.binning;

/**
 * A storage for bins.
 */
interface BinStore<BIN extends Bin> {
    BIN getBin(int binIndex);

    void putBin(BIN bin);
}
