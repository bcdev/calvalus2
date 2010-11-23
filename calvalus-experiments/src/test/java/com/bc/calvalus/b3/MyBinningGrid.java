package com.bc.calvalus.b3;


class MyBinningGrid implements BinningGrid {
    @Override
    public int getBinIndex(double lat, double lon) {
        return (int) lon;
    }

    @Override
    public int getRowIndex(int bin) {
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
