package org.esa.beam.binning;


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
    public long getNumBins() {
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

    @Override
    public double[] getCenterLatLon(long bin) {
        return new double[0];
    }
}
