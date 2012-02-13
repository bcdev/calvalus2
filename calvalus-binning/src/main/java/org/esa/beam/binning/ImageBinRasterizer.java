package org.esa.beam.binning;

import com.bc.calvalus.binning.BinRasterizer;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.binning.WritableVector;

import java.util.Arrays;

/**
* @author Norman Fomferra
*/
public final class ImageBinRasterizer extends BinRasterizer {
    private final int rasterWidth;
    private final int[] bandIndices;
    private final float[][] bandData;

    private final int bandCount;


    public ImageBinRasterizer(int rasterWidth, int rasterHeight, int[] bandIndices) {
        this.rasterWidth = rasterWidth;
        this.bandIndices = bandIndices.clone();
        this.bandCount = bandIndices.length;
        this.bandData = new float[bandCount][rasterWidth * rasterHeight];
        for (int i = 0; i < bandCount; i++) {
            Arrays.fill(bandData[i], Float.NaN);
        }
    }

    public float[][] getBandData() {
        return bandData;
    }

    public float[] getBandData(int bandIndex) {
        return this.bandData[bandIndex];
    }

    @Override
    public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) {
        for (int i = 0; i < bandCount; i++) {
            bandData[i][rasterWidth * y + x] = outputVector.get(bandIndices[i]);
        }
    }

    @Override
    public void processMissingBin(int x, int y) throws Exception {
        for (int i = 0; i < bandCount; i++) {
            bandData[i][rasterWidth * y + x] = Float.NaN;
        }
    }
}
