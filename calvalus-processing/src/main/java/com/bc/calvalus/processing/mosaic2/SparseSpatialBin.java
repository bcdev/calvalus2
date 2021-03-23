package com.bc.calvalus.processing.mosaic2;

import org.apache.hadoop.io.Writable;
import org.esa.snap.binning.SpatialBin;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A SpatialBin for reading and writing as part of a mosaic (that may have null values represented by -1 in the index position).
 *
 * @author Martin
 */
public final class SparseSpatialBin extends SpatialBin implements Writable {

    public void write(DataOutput out) throws IOException {
        out.writeLong(getIndex());
        super.write(out);
    }
}
