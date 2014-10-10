package com.bc.calvalus.processing.l3;

import org.apache.hadoop.io.Writable;
import org.esa.beam.binning.SpatialBin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


/**
 * A Hadoop-serializable, spatial bin.
 * The class is final for allowing method in-lining.
 *
 * @author Norman Fomferra
 * @author Martin
 */
public final class L3SpatialBin extends SpatialBin implements Writable {

    public static final int METADATA_MAGIC_NUMBER = -1;

    String metadata = null;

    @SuppressWarnings("UnusedDeclaration")
    public L3SpatialBin() {
        super();
    }

    public L3SpatialBin(long index, int numFeatures) {
        super(index, numFeatures);
    }

    public L3SpatialBin(String metadata) {
        super(METADATA_MAGIC_NUMBER, 0);
        this.metadata = metadata;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public void write(DataOutput dataOutput) throws IOException {
         // Note, we don't serialise the index, because it is usually the MapReduce key
        if (metadata == null) {
            dataOutput.writeInt(getNumObs());
            dataOutput.writeInt(getFeatureValues().length);
            for (float value : getFeatureValues()) {
                dataOutput.writeFloat(value);
            }
        } else {
            dataOutput.writeInt(METADATA_MAGIC_NUMBER);
            dataOutput.writeUTF(metadata);
        }

     }

     public void readFields(DataInput dataInput) throws IOException {
         // // Note, we don't serialise the index, because it is usually the MapReduce key
         setNumObs(dataInput.readInt());
         if (getNumObs() != METADATA_MAGIC_NUMBER) {
            final int numFeatures = dataInput.readInt();
            if (getFeatureValues() == null || getFeatureValues().length != numFeatures) {
                setNumFeatures(numFeatures);
            }
            for (int i = 0; i < numFeatures; i++) {
                getFeatureValues()[i] = dataInput.readFloat();
            }
         } else {
            metadata = dataInput.readUTF();
         }
     }

     public static SpatialBin read(DataInput dataInput) throws IOException {
         return read(-1L, dataInput);
     }

     public static SpatialBin read(long index, DataInput dataInput) throws IOException {
         SpatialBin bin = new SpatialBin();
         bin.setIndex(index);
         bin.readFields(dataInput);
         return bin;
     }

     @Override
     public String toString() {
         return String.format("%s{index=%d, numObs=%d, featureValues=%s, metadata=%s}",
                              getClass().getSimpleName(), getIndex(), getNumObs(), Arrays.toString(getFeatureValues()), metadata);
     }

}
