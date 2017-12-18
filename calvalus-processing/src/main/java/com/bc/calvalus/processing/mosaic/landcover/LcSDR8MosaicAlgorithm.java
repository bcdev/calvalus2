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
    static final int STATUS_WATER = 2;
    static final int STATUS_HAZE = 11;
    static final int STATUS_BRIGHT = 12;
    static final int STATUS_DARK = 15;
    static final int[] RANK = new int[] { 0,
                                15, 14, 13, 5, 4,
                                -1, -1, -1, -1, -1,
                                8, 9, -1, 6, 7};

    private static final int SAMPLE_INDEX_STATUS = 0;
    private static final int SAMPLE_INDEX_SDR8 = 1;
    private static final int SAMPLE_INDEX_NDVI = 2;
    private static final int SAMPLE_INDEX_TC1 = 2;
    private static final int SAMPLE_INDEX_MJD = 3;
    private static final int NUM_SAMPLE_BANDS = 4;

    private static final int AGG_INDEX_COUNT = 0;
    private static final int AGG_INDEX_SDR_SUM = 1;
    private static final int AGG_INDEX_SDR_SQSUM = 2;
    private static final int AGG_INDEX_STATUS = 3;
    private static final int AGG_INDEX_TC1_COUNT = 4;
    private static final int AGG_INDEX_TC1_SUM = 5;
    private static final int AGG_INDEX_TC1_SQSUM = 6;
    private static final int AGG_INDEX_TC1_STATUS = 7;
    private static final int AGG_INDEX_MJD = 8;
    private static final int AGG_INDEX_TC1_MJD = 9;
    private static final int AGG_INDEX_MAXNDVI = 4;
    private static final int AGG_INDEX_SDR4MAXNDVI = 5;
    private static final int NUM_AGGREGATION_BANDS = 10;

    private int[] varIndexes;
    private float[][] aggregatedSamples = null;
    private String[] featureNames;
    private int tileSize;
    private StatusRemapper statusRemapper;
    private Configuration jobConf;
    private float applyFilterThresh;
    private String temporalCloudBandName;
    private boolean withTc4 = false;

    @Override
    public void initTemporal(TileIndexWritable tileIndex) {
        //System.err.println("initTemporal " + tileIndex + " numElements=" + (tileSize * tileSize));
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[NUM_AGGREGATION_BANDS][numElems];
        for (int band = 0; band < NUM_AGGREGATION_BANDS; band++) {
            Arrays.fill(aggregatedSamples[band], 0.0f);
        }
    }

    /**
     * Ignore observations that are not LAND or BRIGHT or HAZE.
     * If there are some land observations, count their contributions.
     * Else, if there are some bright observations count their contributions.
     * Else, if there are some haze observations count their contributions.
     * Use AGG_INDEX_MAXNDVI for the status memo.
     * @param samples
     */
    @Override
    public void processTemporal(float[][] samples) {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            int status = (int) samples[varIndexes[SAMPLE_INDEX_STATUS]][i];
            float mjd = samples[varIndexes[SAMPLE_INDEX_MJD]][i];
            //int status = StatusRemapper.remapStatus(statusRemapper, status1);
//            if (AbstractLcMosaicAlgorithm.maybeIsPixelPos(2078, 2856, i, tileSize)) {
//                System.err.println("ix=" + (i % tileSize) + " iy=" + (i / tileSize) + " status=" + status + " status=" + status + " tc4=" + samples[varIndexes[SAMPLE_INDEX_SDR8]][i] + " tc1=" + samples[varIndexes[SAMPLE_INDEX_TC1]][i]);
//            }
//            if (status == STATUS_LAND || status == STATUS_BRIGHT || status == STATUS_HAZE) {
            if (status == STATUS_LAND || status == STATUS_BRIGHT || status == STATUS_HAZE || status == STATUS_DARK || status == STATUS_WATER) {
                int oldStatus = (int) aggregatedSamples[AGG_INDEX_STATUS][i];
                if (oldStatus == 0.0f) {
                    aggregatedSamples[AGG_INDEX_STATUS][i] = status;
                    aggregatedSamples[AGG_INDEX_MJD][i] = mjd;
                }
                // accumulate LAND SDRs
                float sdr = samples[varIndexes[SAMPLE_INDEX_SDR8]][i];
                if (!Float.isNaN(sdr)) {
                    aggregatedSamples[AGG_INDEX_COUNT][i]++;
                    aggregatedSamples[AGG_INDEX_SDR_SUM][i] += sdr;
                    aggregatedSamples[AGG_INDEX_SDR_SQSUM][i] += sdr * sdr;
                    if (aggregatedSamples[AGG_INDEX_COUNT][i] > 1 && isNotSameOrbit(mjd, aggregatedSamples[AGG_INDEX_MJD][i])) {
                        aggregatedSamples[AGG_INDEX_MJD][i] = Float.NaN;
                    }
                    if (! withTc4) {
                        float ndvi = samples[varIndexes[SAMPLE_INDEX_NDVI]][i];
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

//            if (withTc4 && (status == STATUS_LAND || status == STATUS_DARK || status == STATUS_WATER)) {
            if (withTc4 && (status == STATUS_LAND || status == STATUS_BRIGHT || status == STATUS_HAZE || status == STATUS_DARK || status == STATUS_WATER)) {
                int oldStatus = (int) aggregatedSamples[AGG_INDEX_TC1_STATUS][i];
                if (oldStatus == 0.0f) {
                    aggregatedSamples[AGG_INDEX_TC1_STATUS][i] = status;
                    aggregatedSamples[AGG_INDEX_TC1_MJD][i] = mjd;
                }
                // accumulate LAND SDRs
                float sdr = samples[varIndexes[SAMPLE_INDEX_TC1]][i];
                if (!Float.isNaN(sdr)) {
                    aggregatedSamples[AGG_INDEX_TC1_COUNT][i]++;
                    aggregatedSamples[AGG_INDEX_TC1_SUM][i] += sdr;
                    aggregatedSamples[AGG_INDEX_TC1_SQSUM][i] += sdr * sdr;
                    if (aggregatedSamples[AGG_INDEX_TC1_COUNT][i] > 1 && isNotSameOrbit(mjd, aggregatedSamples[AGG_INDEX_TC1_MJD][i])) {
                        aggregatedSamples[AGG_INDEX_TC1_MJD][i] = Float.NaN;
                    }
//                    if (AbstractLcMosaicAlgorithm.maybeIsPixelPos(2078, 2856, i, tileSize)) {
//                        System.err.println("ix=" + (i % tileSize) + " iy=" + (i / tileSize) + " adding tc1=" + sdr + " mjd=" + mjd + " aggMjd=" + aggregatedSamples[AGG_INDEX_TC1_MJD][i]);
//                    }
                }
            }
        }
    }

    /**
     * Compares two dates
     * @param mjd1
     * @param mjd2
     * @return whether delta is less than 1 day or one of them is NaN
     */
    private boolean isNotSameOrbit(float mjd1, float mjd2) {
        return ! (Math.abs(mjd1-mjd2) < 1.0f);
    }

    @Override
    public float[][] getTemporalResult() {
        int numElems = tileSize * tileSize;
        float[][] result = new float[2][numElems];
        for (int i = 0; i < numElems; i++) {
            result[0][i] = Float.NaN;
            result[1][i] = Float.NaN;
            float count = aggregatedSamples[AGG_INDEX_COUNT][i];
            if (count >= 2 && Float.isNaN(aggregatedSamples[AGG_INDEX_MJD][i])) {
                double sdrSum = aggregatedSamples[AGG_INDEX_SDR_SUM][i];
                double sdrSqrSum = aggregatedSamples[AGG_INDEX_SDR_SQSUM][i];

                double sdrMean = sdrSum / count;
                double sdrSigma = Math.sqrt(sdrSqrSum / count - sdrMean * sdrMean);
                if (! withTc4) {
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
                } else {
                    double sdrCloudDetector = Math.min(sdrMean - sdrSigma * 1.4, sdrMean * 1.4);
                    result[0][i] = (float) sdrCloudDetector;
                }
//                }
//                if (AbstractLcMosaicAlgorithm.maybeIsPixelPos(2078, 2856, i, tileSize)) {
//                    System.err.println("ix=" + (i % tileSize) + " iy=" + (i / tileSize) + " sdrMean=" + sdrMean + " sdrSigma=" + sdrSigma + " sdrCloud=" + result[0][i] + " sdrShadow=" + result[1][i]);
//                }
                // if "ndvi" instead of sdr_B3 (spot only)
                //if (cloudValue2 > applyFilterThresh) {
                //    float sdrCloudDetector = Math.max(sdrMean * 0.85f, sdrMean - sdrSigma);
                //    result[0][i] = sdrCloudDetector;
                //}
//            } else {
//                if (AbstractLcMosaicAlgorithm.maybeIsPixelPos(2078, 2856, i, tileSize)) {
//                    System.err.println("ix=" + (i % tileSize) + " iy=" + (i / tileSize) + " count=" + count);
//                }
            }
            count = aggregatedSamples[AGG_INDEX_TC1_COUNT][i];
            if (withTc4 && count >= 2 && Float.isNaN(aggregatedSamples[AGG_INDEX_TC1_MJD][i])) {
                double sdrSum = aggregatedSamples[AGG_INDEX_TC1_SUM][i];
                double sdrSqrSum = aggregatedSamples[AGG_INDEX_TC1_SQSUM][i];
                double sdrMean = sdrSum / count;
                double sdrSigma = Math.sqrt(sdrSqrSum / count - sdrMean * sdrMean);
                //double sdrCloudShadowDetector = sdrMean - sdrSigma * 1.35;
                double sdrCloudShadowDetector = (sdrMean - sdrSigma) * 0.9;
                result[1][i] = (float) sdrCloudShadowDetector;
                if (AbstractLcMosaicAlgorithm.maybeIsPixelPos(2078, 2856, i, tileSize)) {
                    System.err.println("ix=" + (i % tileSize) + " iy=" + (i / tileSize) + " tc1Mean=" + sdrMean + " tc1Sigma=" + sdrSigma + " tc1CloudShadow=" + result[1][i]);
                }
            }
        }
        return result;
    }

    @Override
    public void setConf(Configuration jobConf) {
        this.jobConf = jobConf;
        temporalCloudBandName = jobConf.get("calvalus.lc.temporalCloudBandName"); // "sdr_8", "sdr_B3", ...
        applyFilterThresh = Float.parseFloat(jobConf.get("calvalus.lc.temporalCloudFilterThreshold")); // 0.075f
        withTc4 = "tc4".equals(temporalCloudBandName);
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
        if (withTc4) {
            varIndexes[SAMPLE_INDEX_TC1] = getVariableIndex(varCtx, "tc1");
            varIndexes[SAMPLE_INDEX_MJD] = getVariableIndex(varCtx, "mjd");
        } else {
            varIndexes[SAMPLE_INDEX_NDVI] = getVariableIndex(varCtx, "ndvi");
        }
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
