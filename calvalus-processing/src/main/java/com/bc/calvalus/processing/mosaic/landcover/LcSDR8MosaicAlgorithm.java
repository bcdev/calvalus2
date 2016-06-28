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

package com.bc.calvalus.processing.mosaic.landcover;

import com.bc.calvalus.processing.mosaic.DefaultMosaicProductFactory;
import com.bc.calvalus.processing.mosaic.MosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.MosaicGrid;
import com.bc.calvalus.processing.mosaic.MosaicProductFactory;
import com.bc.calvalus.processing.mosaic.TileIndexWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.VariableContext;

import java.util.Arrays;

/**
 * The algorithm, for lc_cci..
 *
 * @author MarcoZ
 */
public class LcSDR8MosaicAlgorithm implements MosaicAlgorithm, Configurable {

    private static final int STATUS_LAND = 1;

    private static final int SAMPLE_INDEX_STATUS = 0;
    private static final int SAMPLE_INDEX_SDR8 = 1;
    private static final int SAMPLE_INDEX_NDVI = 2;
    private static final int NUM_SAMPLE_BANDS = 3;

    private static final int AGG_INDEX_COUNT = 0;
    private static final int AGG_INDEX_SDR_SUM = 1;
    private static final int AGG_INDEX_SDR_SQSUM = 2;
    private static final int AGG_INDEX_MAXNDVI = 3;
    private static final int AGG_INDEX_SDR4MAXNDVI = 4;
    private static final int NUM_AGGREGATION_BANDS = 5;

    private int[] varIndexes;
    private float[][] aggregatedSamples = null;
    private String[] featureNames;
    private int tileSize;
    private StatusRemapper statusRemapper;
    private Configuration jobConf;
    private float applyFilterThresh;
    private String temporalCloudBandName;

    @Override
    public void initTemporal(TileIndexWritable tileIndex) {
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[NUM_AGGREGATION_BANDS][numElems];
        for (int band = 0; band < NUM_AGGREGATION_BANDS; band++) {
            Arrays.fill(aggregatedSamples[band], 0.0f);
        }
    }

    @Override
    public void processTemporal(float[][] samples) {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            int status = (int) samples[varIndexes[SAMPLE_INDEX_STATUS]][i];
            status = StatusRemapper.remapStatus(statusRemapper, status);
            if (status == STATUS_LAND) {
                // Since we have seen LAND now, accumulate LAND SDRs
                float sdr = samples[varIndexes[SAMPLE_INDEX_SDR8]][i];
                float ndvi = samples[varIndexes[SAMPLE_INDEX_NDVI]][i];
                if (!Float.isNaN(sdr)) {
                    aggregatedSamples[AGG_INDEX_COUNT][i]++;
                    aggregatedSamples[AGG_INDEX_SDR_SUM][i] += sdr;
                    aggregatedSamples[AGG_INDEX_SDR_SQSUM][i] += sdr * sdr;

                    if (aggregatedSamples[AGG_INDEX_COUNT][i] == 1) {
                        // first pixel
                        aggregatedSamples[AGG_INDEX_MAXNDVI][i] = ndvi;
                        aggregatedSamples[AGG_INDEX_SDR4MAXNDVI][i] = sdr;
                    } else if (ndvi > aggregatedSamples[AGG_INDEX_MAXNDVI][i]) {
                        aggregatedSamples[AGG_INDEX_MAXNDVI][i] = ndvi;
                        aggregatedSamples[AGG_INDEX_SDR4MAXNDVI][i] = sdr;
                    }
                }
            }
        }
    }

    @Override
    public float[][] getTemporalResult() {
        int numElems = tileSize * tileSize;
        float[][] result = new float[2][numElems];
        for (int i = 0; i < numElems; i++) {
            result[0][i] = Float.NaN;
            result[1][i] = Float.NaN;
            float count = aggregatedSamples[AGG_INDEX_COUNT][i];
            if (count >= 2) {
                double sdrSum = aggregatedSamples[AGG_INDEX_SDR_SUM][i];
                double sdrSqrSum = aggregatedSamples[AGG_INDEX_SDR_SQSUM][i];

                double sdrMean = sdrSum / count;
                double sdrSigma = Math.sqrt(sdrSqrSum / count - sdrMean * sdrMean);
                double tau1 = sdrSigma / sdrMean;


                if (tau1 > applyFilterThresh) {
                    double tau2 = sdrMean + sdrSigma;
                    double tau3 = sdrMean * 1.35;
                    double sdr4MaxNdvi = aggregatedSamples[AGG_INDEX_SDR4MAXNDVI][i];
                    double tau4 = sdr4MaxNdvi + 2 * sdrSigma;
                    double tau5 = sdrMean - sdrSigma;
                    double tau6 = sdrMean * 0.65;
                    double sdrCloudDetector = Math.min(Math.min(tau3, tau2), tau4);
                    double sdrCloudShadowDetector = Math.min(tau5, tau6);
                    result[0][i] = (float) sdrCloudDetector;
                    result[1][i] = (float) sdrCloudShadowDetector;
                }
                // if "ndvi" instead of sdr_B3 (spot only)
                //if (cloudValue2 > applyFilterThresh) {
                //    float sdrCloudDetector = Math.max(sdrMean * 0.85f, sdrMean - sdrSigma);
                //    result[0][i] = sdrCloudDetector;
                //}
            }
        }
        return result;
    }

    @Override
    public void setConf(Configuration jobConf) {
        this.jobConf = jobConf;
        temporalCloudBandName = jobConf.get("calvalus.lc.temporalCloudBandName"); // "sdr_8", "sdr_B3", ...
        applyFilterThresh = Float.parseFloat(jobConf.get("calvalus.lc.temporalCloudFilterThreshold")); // 0.075f
    }

    @Override
    public Configuration getConf() {
        return jobConf;
    }

    @Override
    public void setVariableContext(VariableContext variableContext) {
        varIndexes = createVariableIndexes(variableContext);
        featureNames = createOutputFeatureNames();
        tileSize = MosaicGrid.create(jobConf).getTileSize();
        statusRemapper = StatusRemapper.create(jobConf);
    }

    @Override
    public String[] getTemporalFeatures() {
        return featureNames;
    }

    @Override
    public float[][] getOutputResult(float[][] temporalData) {
        return temporalData;
    }

    @Override
    public String[] getOutputFeatures() {
        return featureNames;
    }

    @Override
    public MosaicProductFactory getProductFactory() {
        return new DefaultMosaicProductFactory(getTemporalFeatures());
    }

    private int[] createVariableIndexes(VariableContext varCtx) {
        int[] varIndexes = new int[NUM_SAMPLE_BANDS];
        varIndexes[SAMPLE_INDEX_STATUS] = getVariableIndex(varCtx, "status");
        varIndexes[SAMPLE_INDEX_SDR8] = getVariableIndex(varCtx, temporalCloudBandName);
        varIndexes[SAMPLE_INDEX_NDVI] = getVariableIndex(varCtx, "ndvi");
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
        return new String[]{"sdr_cloud_detector", "sdr_cloud_shadow_detector"};
    }
}
