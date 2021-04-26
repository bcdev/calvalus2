package com.bc.calvalus.processing.l3;

import org.apache.hadoop.io.Writable;
import org.esa.snap.binning.SpatialBin;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A Hadoop-serializable, spatial bin for JD, packs 3 relevant values into short to save space in Hadoop streaming and sorting
 *
 * @author Martin
 */
public class PackedJDSpatialBin extends L3SpatialBin implements Writable {

    @SuppressWarnings("UnusedDeclaration")
    public PackedJDSpatialBin() {
        super();
    }

    public PackedJDSpatialBin(long index, int numFeatures, int numGrowableFeatures) {
        super(index, numFeatures, numGrowableFeatures);
    }

    public PackedJDSpatialBin(String metadata) {
        super(metadata);
    }

    public void write(DataOutput dataOutput) throws IOException {
        if (metadata == null) {
            dataOutput.writeShort((int) getFeatureValues()[0]);            // mjd2000 in days
            dataOutput.writeShort((int) getFeatureValues()[1]);            // jd
            dataOutput.writeShort((int) (getFeatureValues()[2]*100+0.1));  // cl
        } else {
            dataOutput.writeShort(L3SpatialBin.METADATA_MAGIC_NUMBER);
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
         setNumObs(1);
         short v0 = dataInput.readShort();
         if (v0 != L3SpatialBin.METADATA_MAGIC_NUMBER) {
             setNumFeatures(3);
             getFeatureValues()[0] = v0;                                     // mjd2000 in days
             getFeatureValues()[1] = dataInput.readShort();                  // jd
             getFeatureValues()[2] = ((float) dataInput.readShort()) / 100;  // cl
         } else {
             int noOfChunks = dataInput.readInt();
             StringBuffer accu = new StringBuffer();
             for (int i=0; i<noOfChunks; ++i) {
                 accu.append(dataInput.readUTF());
             }
             metadata = accu.toString();
         }
     }
}
