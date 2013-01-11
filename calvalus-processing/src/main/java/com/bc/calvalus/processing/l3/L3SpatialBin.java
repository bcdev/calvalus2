package com.bc.calvalus.processing.l3;

import org.apache.hadoop.io.Writable;
import org.esa.beam.binning.SpatialBin;


/**
 * A Hadoop-serializable, spatial bin.
 * The class is final for allowing method in-lining.
 *
 * @author Norman Fomferra
 */
public final class L3SpatialBin extends SpatialBin implements Writable {

    @SuppressWarnings("UnusedDeclaration")
    public L3SpatialBin() {
        super();
    }

    public L3SpatialBin(long index, int numFeatures) {
        super(index, numFeatures);
    }

}
