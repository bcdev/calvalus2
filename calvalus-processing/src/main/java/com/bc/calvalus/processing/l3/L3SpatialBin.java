package com.bc.calvalus.processing.l3;

import org.apache.hadoop.io.Writable;
import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.support.GrowableVector;

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
public class L3SpatialBin extends SpatialBin implements Writable {

    public static final int METADATA_MAGIC_NUMBER = -1;

    String metadata = null;

    @SuppressWarnings("UnusedDeclaration")
    public L3SpatialBin() {
        super();
    }

    public L3SpatialBin(long index, int numFeatures, int numGrowableFeatures) {
        super(index, numFeatures, numGrowableFeatures);
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
            super.write(dataOutput);
        } else {
            dataOutput.writeInt(METADATA_MAGIC_NUMBER);
            int chunkSize = 65535 / 3;  // UTF may blow up the string to trice the size in bytes
            int noOfChunks = (metadata.length() + chunkSize - 1) / chunkSize;
            dataOutput.writeInt(noOfChunks);
            int chunkStart = 0;
            for (int i=0; i<noOfChunks; ++i) {
                int chunkStop = Math.min((i+1)*chunkSize, metadata.length());
                dataOutput.writeUTF(metadata.substring(chunkStart, chunkStop));
                chunkStart = chunkStop;
            }
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

             final int numVectors = dataInput.readInt();
             vectors = new GrowableVector[numVectors];
             for (int i = 0; i < numVectors; i++) {
                 final int vectorLength = dataInput.readInt();
                 final GrowableVector vector = new GrowableVector(vectorLength);
                 vectors[i] = vector;
                 for (int k = 0; k < vectorLength; k++) {
                     vector.add(dataInput.readFloat());
                 }
             }
         } else {
             int noOfChunks = dataInput.readInt();
             StringBuffer accu = new StringBuffer();
             for (int i=0; i<noOfChunks; ++i) {
                 accu.append(dataInput.readUTF());
             }
             metadata = accu.toString();
         }
     }

     public static SpatialBin read(DataInput dataInput) throws IOException {
         return read(-1L, dataInput);
     }

     public static SpatialBin read(long index, DataInput dataInput) throws IOException {
         L3SpatialBin bin = new L3SpatialBin();
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
