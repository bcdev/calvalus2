package com.bc.calvalus.processing.fire.format.grid;

import java.io.IOException;

interface FireGridDataSource {

    /**
     * Reads the input pixels for the target pixel given by x, y, and stores the result in a SourceData object.
     *
     * @param x The target pixel x
     * @param y The target pixel y
     * @return A source data object
     * @throws IOException
     */
    SourceData readPixels(int x, int y) throws IOException;

    void setDoyFirstOfMonth(int doyFirstOfMonth);

    void setDoyLastOfMonth(int doyLastOfMonth);

}
