/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package org.esa.beam.binning;

/**
 * The grid used for the binning.
 *
 * @author Norman Fomferra
 */
public interface BinningGrid {
    /**
     * Transforms a geographical point into a unique bin index.
     *
     * @param lat The latitude in degrees.
     * @param lon The longitude in degrees.
     * @return The unique bin index.
     */
    long getBinIndex(double lat, double lon);

    /**
     * Gets the row index for the given bin index. The pseudo code for the implementation is
     * <pre>
     *       int row = numRows - 1;
     *       while (idx < baseBin[row]) {
     *            row--;
     *       }
     *       return row;
     * </pre>
     *
     * @param bin The bin index.
     * @return The row index.
     */
    int getRowIndex(long bin);

    /**
     * Gets the total number of bins in the binning grid.
     *
     * @return The total number of bins.
     */
    long getNumBins();

    /**
     * Gets the number of rows in this grid.
     *
     * @return The number of rows.
     */
    int getNumRows();

    /**
     * Gets the number of columns in the given row.
     *
     * @param row The row index.
     * @return The number of columns.
     */
    int getNumCols(int row);

    /**
     * Gets geographical longitude and latitude (in this order) for the center of the given bin.
     *
     * @param bin The bin index.
     * @return longitude and latitude (in this order)
     */
    double[] getCenterLonLat(long bin);
}
