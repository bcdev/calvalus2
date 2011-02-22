package com.bc.calvalus.binning;


class MyBinningGrid implements BinningGrid {
    @Override
    public long getBinIndex(double lat, double lon) {
        return (int) lon;
    }

    @Override
    public int getRowIndex(long bin) {
        return 0;
    }

    @Override
    public int getNumRows() {
        return 1;
    }

    @Override
    public int getNumCols(int row) {
        return 1;
    }
}
