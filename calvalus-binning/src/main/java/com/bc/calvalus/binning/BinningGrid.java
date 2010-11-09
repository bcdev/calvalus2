package com.bc.calvalus.binning;


public interface BinningGrid {
    int getBinIndex(double lat, double lon);

    // double[] getCenterLatLon(int idx);
}
