package com.bc.calvalus.processing.fire;

import org.esa.snap.binning.Vector;
import org.esa.snap.binning.support.VectorImpl;

/**
 *
 * @author thomas
 */
public class GridCell {

    private static final int NUM_LC_CLASSES = 18;

    Vector[] data;

    public GridCell() {
        this.data = new Vector[4 + NUM_LC_CLASSES];
    }

    public Vector getData(int bandId) {
        return data[bandId];
    }

    public void setData(int bandId, float[] data) {
        this.data[bandId] = new VectorImpl(data);
    }



}
