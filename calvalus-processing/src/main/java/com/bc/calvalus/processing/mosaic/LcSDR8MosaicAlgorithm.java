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

import java.util.Arrays;

/**
 * The algorithm, for lc_cci..
 *
 * @author MarcoZ
 */
public class LcSDR8MosaicAlgorithm implements MosaicAlgorithm {

    private static final int STATUS_LAND = 1;

    private int[] varIndexes;
    private float[][] aggregatedSamples = null;
    private String[] outputFeatures;
    private int tileSize;
    private int variableCount;

    @Override
    public void init() {
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[variableCount][numElems];
        for (int band = 0; band < variableCount; band++) {
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
                aggregatedSamples[0][i]++;
                aggregatedSamples[1][i] += sdr8;
                aggregatedSamples[2][i] += sdr8 * sdr8;
            }
        }
    }

    @Override
    public float[][] getResult() {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            float count = aggregatedSamples[0][i];
            if (count > 0) {
                float sdr8Sum = aggregatedSamples[1][i];
                float sdr8SqrSum = aggregatedSamples[2][i];

                float sdr8Mean = sdr8Sum / count;
                float sdr8Sigma = (float) Math.sqrt(sdr8SqrSum / count - sdr8Mean * sdr8Mean);

                aggregatedSamples[1][i] = sdr8Mean;
                aggregatedSamples[2][i] = sdr8Sigma;
            } else {
                aggregatedSamples[0][i] = Float.NaN;
                aggregatedSamples[1][i] = Float.NaN;
                aggregatedSamples[2][i] = Float.NaN;
            }
        }
        return aggregatedSamples;
    }

    @Override
    public void setVariableContext(VariableContext variableContext) {
        varIndexes = createVariableIndexes(variableContext);
        outputFeatures = createOutputFeatureNames();
        variableCount = outputFeatures.length;
        tileSize = new MosaicGrid().getTileSize();
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
        String[] featureNames = new String[3];
        int j = 0;
        featureNames[j++] = "land_count";
        featureNames[j++] = "sdr_8_x";
        featureNames[j++] = "sdr_8_xx";
        return featureNames;
    }
}
