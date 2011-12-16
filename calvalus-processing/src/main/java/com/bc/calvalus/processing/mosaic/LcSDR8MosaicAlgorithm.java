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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.binning.VariableContext;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;

/**
 * The algorithm, for lc_cci..
 *
 * @author MarcoZ
 */
public class LcSDR8MosaicAlgorithm implements MosaicAlgorithm, Configurable {

    private static final int STATUS_LAND = 1;
    private static final int NUM_AGGREGATION_BANDS = 3;

    private int[] varIndexes;
    private float[][] aggregatedSamples = null;
    private String[] outputFeatures;
    private int tileSize;
    private Configuration jobConf;

    @Override
    public void init(TileIndexWritable tileIndex) {
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[NUM_AGGREGATION_BANDS][numElems];
        for (int band = 0; band < NUM_AGGREGATION_BANDS; band++) {
            Arrays.fill(aggregatedSamples[band], 0.0f);
        }
    }

    @Override
    public void process(float[][] samples) {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            final int status = (int) samples[varIndexes[0]][i];
            if (status == STATUS_LAND) {
                // Since we have seen LAND now, accumulate LAND SDRs
                float sdr8 = samples[varIndexes[1]][i];
                if (!Float.isNaN(sdr8)) {
                    aggregatedSamples[0][i]++;
                    aggregatedSamples[1][i] += sdr8;
                    aggregatedSamples[2][i] += sdr8 * sdr8;
                }
            }
        }
    }

    @Override
    public float[][] getResult() {
        int numElems = tileSize * tileSize;
        float[][] result = new float[1][numElems];
        for (int i = 0; i < numElems; i++) {
            result[0][i] = Float.NaN;
            float count = aggregatedSamples[0][i];
            if (count >= 2) {
                float sdr8Sum = aggregatedSamples[1][i];
                float sdr8SqrSum = aggregatedSamples[2][i];

                float sdr8Mean = sdr8Sum / count;
                float sdr8Sigma = (float) Math.sqrt(sdr8SqrSum / count - sdr8Mean * sdr8Mean);
                float cloudValue2 = sdr8Sigma / sdr8Mean;
                if (cloudValue2 > 0.2f) {
                    float sdr8CloudDetector = sdr8Mean + sdr8Sigma;
                    result[0][i] = sdr8CloudDetector;
                }
            }
        }
        return result;
    }

    @Override
    public void setConf(Configuration jobConf) {
        this.jobConf = jobConf;
    }

    @Override
    public Configuration getConf() {
        return jobConf;
    }

    @Override
    public void setVariableContext(VariableContext variableContext) {
        varIndexes = createVariableIndexes(variableContext);
        outputFeatures = createOutputFeatureNames();
        tileSize = MosaicGrid.create(jobConf).getTileSize();
    }

    @Override
    public String[] getOutputFeatures() {
        return outputFeatures;
    }

    private static int[] createVariableIndexes(VariableContext varCtx) {
        int[] varIndexes = new int[2];
        varIndexes[0] = getVariableIndex(varCtx, "status");
        varIndexes[1] = getVariableIndex(varCtx, "sdr_8");
        return varIndexes;
    }

    private static int getVariableIndex(VariableContext varCtx, String varName) {
        int varIndex = varCtx.getVariableIndex(varName);
        if (varIndex < 0) {
            throw new IllegalArgumentException(String.format("varIndex < 0 for varName='%s'", varName));
        }
        return varIndex;
    }

    private static String[] createOutputFeatureNames() {
        return new String[]{"sdr_8_cloud_detector"};
    }
}
