/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic2;

import org.apache.hadoop.io.CompressedWritable;
import org.esa.snap.binning.SpatialBin;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A Hadoop writable for a tile of SpatialBins.
 *
 * @author Martin
 */
public class L3SpatialBinMicroTileWritable extends CompressedWritable {

    private static final int NULL_BIN = -1;
    private String metadata;
    private SpatialBin[] sampleValues;

    public L3SpatialBinMicroTileWritable() {}
    public L3SpatialBinMicroTileWritable(SpatialBin[] sampleValues) {
        this.sampleValues = sampleValues;
    }
    public L3SpatialBinMicroTileWritable(String metadata) { this.metadata = metadata; }

    public SpatialBin[] getSamples() {
        ensureInflated();
        return sampleValues;
    }
    public String getMetadata() {
        ensureInflated();
        return metadata;
    }

    @Override
    public void writeCompressed(DataOutput out) throws IOException {
        if (sampleValues != null) {
            out.writeInt(sampleValues.length);
            for (SpatialBin bin : sampleValues) {
                if (bin != null) {
                    bin.write(out);
                } else {
                    out.writeLong(NULL_BIN);
                }
            }
        } else if (metadata != null) {
            int chunkSize = 65535 / 3;  // UTF may blow up the string to trice the size in bytes
            int noOfChunks = (metadata.length() + chunkSize - 1) / chunkSize;
            out.writeInt(-noOfChunks);
            int chunkStart = 0;
            for (int i=0; i<noOfChunks; ++i) {
                int chunkStop = Math.min((i+1)*chunkSize, metadata.length());
                out.writeUTF(metadata.substring(chunkStart, chunkStop));
                chunkStart = chunkStop;
            }
        } else {
            out.writeInt(0);
        }
    }

    @Override
    public void readFieldsCompressed(DataInput in) throws IOException {
        int numSamples = in.readInt();
        if (numSamples >= 0) {
            sampleValues = new SpatialBin[numSamples];
            for (int i=0; i<numSamples; ++i) {
                long index = in.readLong();
                if (index != NULL_BIN) {
                    sampleValues[i] = SpatialBin.read(in);
                    sampleValues[i].setIndex(index);
                }
            }
        } else {
             StringBuffer accu = new StringBuffer();
             for (int i=0; i<-numSamples; ++i) {
                 accu.append(in.readUTF());
             }
             metadata = accu.toString();
        }
    }

    public String toString() {
        ensureInflated();
        if (sampleValues != null) {
            return "L3SpatialBinMicroTileWritable(" + sampleValues.length + ")";
        } else {
            return "L3SpatialBinMicroTileWritable(metadata)";
        }
    }
}



