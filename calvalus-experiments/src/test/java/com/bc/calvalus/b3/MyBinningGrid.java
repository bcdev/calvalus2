package com.bc.calvalus.b3;


class MyBinningGrid implements BinningGrid {
    @Override
    public int getBinIndex(double lat, double lon) {
        return (int) lon;
    }
}
