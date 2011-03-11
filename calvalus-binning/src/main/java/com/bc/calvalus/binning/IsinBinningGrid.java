package com.bc.calvalus.binning;

import static java.lang.Math.cos;
import static java.lang.Math.toRadians;

/**
 * Implementation of the ISIN (Integerized Sinusoidal) binning grid as used for NASA
 * SeaDAS and MODIS L3 products.
 * <p/>
 * The only difference to the original NASA binning grid is, that the bin indices increase from top left to bottom right starting from zero.
 * The NASA grid counts the opposite way. The conversion is thus: {@code idxNasa = numBins - (idx + 1)}.
 *
 * @author Norman Fomferra
 * @author Marco Zühlke
 * @see <a href="http://oceancolor.gsfc.nasa.gov/SeaWiFS/TECH_REPORTS/PreLPDF/PreLVol32.pdf">SeaWiFS Technical Report Series Volume 32, Level-3 SeaWiFS Data</a>
 * @see <a href="http://oceancolor.gsfc.nasa.gov/DOCS/Ocean_Level-3_Binned_Data_Products.pdf">Ocean Level-3 Binned Data Products</a>
 */
public final class IsinBinningGrid implements BinningGrid {
    public static final int DEFAULT_NUM_ROWS = 2160;

    private final int numRows;
    private final double[] latBin;  // latitude of first bin in row
    private final long[] baseBin; // bin-index of the first bin in this row
    private final int[] numBin;  // number of bins in this row
    private final long numBins;

    public IsinBinningGrid() {
        this(DEFAULT_NUM_ROWS);
    }

    public IsinBinningGrid(int numRows) {
        if (numRows < 2) {
            throw new IllegalArgumentException("numRows < 2");
        }
        if (numRows % 2 != 0) {
            throw new IllegalArgumentException("numRows % 2 != 0");
        }

        this.numRows = numRows;
        latBin = new double[numRows];
        baseBin = new long[numRows];
        numBin = new int[numRows];
        baseBin[0] = 0;
        for (int row = 0; row < numRows; row++) {
            latBin[row] = 90.0 - ((row + 0.5) * 180.0 / numRows);
            numBin[row] = (int) (0.5 + (2 * numRows * cos(toRadians(latBin[row]))));
            if (row > 0) {
                baseBin[row] = baseBin[row - 1] + numBin[row - 1];
            }
        }
        numBins = baseBin[numRows - 1] + numBin[numRows - 1];
    }

    @Override
    public int getNumRows() {
        return numRows;
    }

    @Override
    public int getNumCols(int row) {
        return numBin[row];
    }

    @Override
    public long getNumBins() {
        return numBins;
    }

    @Override
    public long getBinIndex(double lat, double lon) {
        final int row = getRowIndex(lat);
        final int col = getColIndex(lon, row);
        return baseBin[row] + col;
    }

    /**
     * Pseudo-code:
     * <pre>
     *       int row = numRows - 1;
     *       while (idx < baseBin[row]) {
     *            row--;
     *       }
     *       return row;
     * </pre>
     *
     * @param binIndex The bin ID.
     * @return The row index.
     */
    @Override
    public int getRowIndex(long binIndex) {
        // compute max constant
        final int max = baseBin.length - 1;
        // avoid field access from the while loop
        final long[] rowBinIds = this.baseBin;
        int low = 0;
        int high = max;
        while (true) {
            int mid = (low + high) >>> 1;
            if (binIndex < rowBinIds[mid]) {
                high = mid - 1;
            } else if (mid == max) {
                return mid;
            } else if (binIndex < rowBinIds[mid + 1]) {
                return mid;
            } else {
                low = mid + 1;
            }
        }
    }

    @Override
    public double[] getCenterLonLat(long binIndex) {
        final int row = getRowIndex(binIndex);
        return new double[]{
                getCenterLon(row, (int) (binIndex - baseBin[row])),
                latBin[row]
        };
    }

    public double[] getCenterLatLon(int row, int col) {
        return new double[]{
                getCenterLon(row, col),
                latBin[row]
        };
    }


    public double getCenterLon(int row, int col) {
        return 360.0 * (col + 0.5) / numBin[row] - 180.0;
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
            return numRows - 1;
        }
        if (lat >= 90.0) {
            return 0;
        }
        return (numRows - 1) - (int) ((90.0 + lat) * (numRows / 180.0));
    }

}
