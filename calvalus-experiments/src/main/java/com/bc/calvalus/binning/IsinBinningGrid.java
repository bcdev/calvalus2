package com.bc.calvalus.binning;

import static java.lang.Math.cos;
import static java.lang.Math.toRadians;

/**
 * Implementation of the ISIN (Integerized Sinusoidal) binnning grid as used for NASA
 * SeaDAS and MODIS L3 products.
 * @see <a href="http://oceancolor.gsfc.nasa.gov/SeaWiFS/TECH_REPORTS/PreLPDF/PreLVol32.pdf">SeaWiFS Technical Report Series Volume 32, Level-3 SeaWiFS Data</a>
 * @see <a href="http://oceancolor.gsfc.nasa.gov/DOCS/Ocean_Level-3_Binned_Data_Products.pdf">Ocean Level-3 Binned Data Products</a>
 */
public final class IsinBinningGrid implements BinningGrid {
    public static final int DEFAULT_NUM_ROWS = 2160;

    private final int numRows;
    private final double[] latBin;
    private final int[] baseBin;
    private final int[] numBin;
    private final int numBins;

    public IsinBinningGrid() {
        this(DEFAULT_NUM_ROWS);
    }

    public IsinBinningGrid(int numRows) {
        this.numRows = numRows;
        latBin = new double[numRows];
        baseBin = new int[numRows];
        numBin = new int[numRows];
        baseBin[0] = 0;
        for (int row = 0; row < numRows; row++) {
            latBin[row] = ((row + 0.5) * 180.0 / numRows) - 90.0;
            numBin[row] = (int) (0.5 + (2 * numRows * cos(toRadians(latBin[row]))));
            if (row > 0) {
                baseBin[row] = baseBin[row - 1] + numBin[row - 1];
            }
        }
        numBins = baseBin[numRows - 1] + numBin[numRows - 1];
    }

    public int getNumRows() {
        return numRows;
    }

    public int getNumCols(int row) {
        return numBin[row];
    }

    public int getNumBins() {
        return numBins;
    }

    @Override
    public int getBinIndex(double lat, double lon) {
        final int row = getRowIndex(lat);
        final int col = getColIndex(lon, row);
        return baseBin[row] + col;
    }

    public int getColIndex(double lon, int row) {
        if (lon <= -180.0) {
            return 0;
        }
        if (lon >= 180.0) {
            return numBin[row] - 1;
        }
        return (int) ((180.0 + lon) * numBin[row] / 360.0);
    }

    public int getRowIndex(double lat) {
        if (lat <= -90.0) {
            return 0;
        }
        if (lat >= 90.0) {
            return numRows - 1;
        }
        return (int) ((90.0 + lat) * (numRows / 180.0));
    }

    public int getRowIndex(int idx) {
        // todo - optimize me!
        int row = numRows - 1;
        while (idx < baseBin[row]) {
            row--;
        }
        return row;
    }

    // @Override
    public double[] getCenterLatLon(int idx) {
        final int row = getRowIndex(idx);
        return new double[]{
                latBin[row],
                getCenterLon(row, idx - baseBin[row])
        };
    }

    public double[] getCenterLatLon(int row, int col) {
        return new double[]{
                latBin[row],
                getCenterLon(row, col)
        };
    }


    public double getCenterLon(int row, int col) {
        return 360.0 * (col + 0.5) / numBin[row] - 180.0;
    }


}
