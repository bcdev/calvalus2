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

import com.bc.calvalus.processing.mosaic.MosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.MosaicGrid;
import com.bc.calvalus.processing.mosaic.MosaicPartitioner;
import com.bc.calvalus.processing.mosaic.TileDataWritable;
import com.bc.calvalus.processing.mosaic.TileIndexWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.esa.snap.binning.VariableContext;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;

/**
 * The algorithm, for lc_cci..
 *
 * @author MarcoZ
 */
abstract public class AbstractLcMosaicAlgorithm implements MosaicAlgorithm, Configurable {

    static final int STATUS_INVALID = 0;
    static final int STATUS_LAND = 1;
    static final int STATUS_WATER = 2;
    static final int STATUS_SNOW = 3;
    static final int STATUS_CLOUD = 4;
    static final int STATUS_CLOUD_SHADOW = 5;
    static final int STATUS_HAZE = 11;
    static final int STATUS_BRIGHT = 12;
    static final int STATUS_TEMPORAL_CLOUD = 14;
    static final int STATUS_DARK = 15;

    public static final String CALVALUS_LC_SDR8_MEAN = "calvalus.lc.sdr8mean";

    protected final int STATUS_BAND_INDEX = 0;
    protected final int SDR_L2_OFFSET = 1;
    protected final int SDR_AGGREGATED_OFFSET = getCounterNames().length + 1;

    abstract protected String[] getCounterNames();

    protected LcL3SensorConfig sensorConfig = null;
    protected boolean bestPixelAggregation = false;
    protected int[] varIndexes;
    protected StatusRemapper statusRemapper;
    protected boolean isTemporalTc4Based;
    protected int temporalCloudBandIndex;

    // output bands * pixels array, output bands = status,5*count,n*sr,ndvi,m*uncertainty
    // MERIS: n=13 and m=13, AVHRR: n=5 and m=2, PROBA: n=4 and m=4
    protected float[][] aggregatedSamples = null;
    protected int[] deepWaterCounter = null;
    protected String[] featureNames;
    protected int tileSize;
    protected int variableCount;

    protected Configuration jobConf;
    protected SequenceFile.Reader reader;
    protected TileIndexWritable sdrCloudKey;
    protected TileDataWritable sdrCloudData;
    protected float[][] sdrCloudDataSamples;


    @Override
    public void setVariableContext(VariableContext variableContext) {
        varIndexes = sensorConfig.createVariableIndexes(variableContext);
        featureNames = sensorConfig.createOutputFeatureNames();
        variableCount = featureNames.length;
        tileSize = MosaicGrid.create(jobConf).getTileSize();
        statusRemapper = StatusRemapper.create(jobConf);
        isTemporalTc4Based = "tc4".equals(jobConf.get("calvalus.lc.temporalCloudBandName"));
        temporalCloudBandIndex = variableContext.getVariableIndex(jobConf.get("calvalus.lc.temporalCloudBandName"));
    }

    @Override
    public void initTemporal(TileIndexWritable tileIndex) throws IOException {
        //System.err.println("initTemporal " + tileIndex + " numElements=" + (tileSize * tileSize));
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[variableCount][numElems];
        deepWaterCounter = new int[numElems];
        for (int band = 0; band < variableCount; band++) {
            Arrays.fill(aggregatedSamples[band], 0.0f);
        }
        if (reader == null && jobConf.get(CALVALUS_LC_SDR8_MEAN) != null) {
            openSdr8MeanReader(getPartition(tileIndex));
        }
        readSdrCloudMeanData(tileIndex);
    }

    protected void readSdrCloudMeanData(TileIndexWritable tileIndex) throws IOException {
        if (reader != null) {
            sdrCloudDataSamples = null;
            if (sdrCloudKey.equals(tileIndex)) {
                reader.getCurrentValue(sdrCloudData);
                sdrCloudDataSamples = sdrCloudData.getSamples();
            } else if (sdrCloudKey.compareTo(tileIndex) == 1) {
                // sdrCloudKey > tileIndex
            } else {
                // sdrCloudKey < tileIndex
                while (reader.next(sdrCloudKey)) {
                    if (sdrCloudKey.equals(tileIndex)) {
                        reader.getCurrentValue(sdrCloudData);
                        sdrCloudDataSamples = sdrCloudData.getSamples();
                        break;
                    } else if (sdrCloudKey.compareTo(tileIndex) == 1) {
                        // sdrCloudKey > tileIndex
                        break;
                    }
                }
            }
        }
    }

    protected int getPartition(TileIndexWritable tileIndex) {
        MosaicPartitioner mosaicPartitioner = new MosaicPartitioner();
        mosaicPartitioner.setConf(jobConf);
        int numPartitions = jobConf.getInt("mapred.reduce.tasks", 1);
        return mosaicPartitioner.getPartition(tileIndex, null, numPartitions);
    }

    public static boolean maybeIsPixelPos(int x, int y, int i, int tileSize) {
        int ix = x % tileSize;
        int iy = y % tileSize;
        return i == iy * tileSize + ix;
    }

    @Override
    public void processTemporal(float[][] samples) {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            int status = (int) samples[varIndexes[0]][i];
            int oldStatus = (int) aggregatedSamples[STATUS_BAND_INDEX][i];

            //check for invalid pixels
            float sdr_1 = samples[varIndexes[SDR_L2_OFFSET]][i];
            if ((status == STATUS_LAND || status == STATUS_SNOW) && Float.isNaN(sdr_1)) {
                status = STATUS_INVALID;
            }

            // TODO: temporarily removed, as it maps haze to cloud too early
            // status = StatusRemapper.remapStatus(statusRemapper, status);

            if (sdrCloudDataSamples != null) {
                if (maybeIsPixelPos(9280, 4656, i, tileSize)
                        || maybeIsPixelPos(4400, 9251, i, tileSize)) {
                    System.err.println("ix=" + (i % tileSize) + " iy=" + (i / tileSize) + " apply temporal cloud filter");
                }
                // temporal test
                if (isTemporalTc4Based) {
                    if (status == STATUS_LAND || status == STATUS_BRIGHT || status == STATUS_HAZE) {
                        float tc4CloudThreshold = sdrCloudDataSamples[0][i];
                        if (!Float.isNaN(tc4CloudThreshold)) {
                            float B2_ac = samples[varIndexes[SDR_L2_OFFSET+1]][i];
                            float B3_ac = samples[varIndexes[SDR_L2_OFFSET+2]][i];
                            float B4_ac = samples[varIndexes[SDR_L2_OFFSET+3]][i];
                            float B8A_ac = samples[varIndexes[SDR_L2_OFFSET+8]][i];
                            float B11_ac = samples[varIndexes[SDR_L2_OFFSET+9]][i];
                            float B12_ac = samples[varIndexes[SDR_L2_OFFSET+10]][i];
                            float tc4 = (float) (-0.8239*B2_ac + 0.0849*B3_ac + 0.4396*B4_ac - 0.058*B8A_ac + 0.2013*B11_ac - 0.2773*B12_ac);
                            if (tc4 < tc4CloudThreshold) {
                                status = STATUS_TEMPORAL_CLOUD;
                            } else if (status == STATUS_BRIGHT || status == STATUS_HAZE) {
                                // we observe that the feature is stable over time, we assume it is clear
                                status = STATUS_LAND;
                            }
//                            if (AbstractLcMosaicAlgorithm.maybeIsPixelPos(2078, 2856, i, tileSize)) {
//                                System.err.println("ix=" + (i%tileSize) + " iy=" + (i/tileSize) + " tc4=" + tc4 + " tc4CloudThreshold=" + tc4CloudThreshold + " status=" + status);
//                            }
//                        } else {
//                            if (AbstractLcMosaicAlgorithm.maybeIsPixelPos(2078, 2856, i, tileSize)) {
//                                System.err.println("ix=" + (i%tileSize) + " iy=" + (i/tileSize) + " tc4CloudThreshold=" + tc4CloudThreshold + " status=" + status);
//                            }
                        }
                    }

                    //TC1 = 0.3029*B2_ac + 0.2786*B3_ac + 0.4733*B4_ac + 0.5599*B8A_ac + 0.508*B11_ac + 0.1872*B12_ac
                    //TC1_tresh = 0.9*(TC1_mean - TC1_stddev)
                    //Shadow: TC1 < TC1_tresh
                    if (status == STATUS_LAND || status == STATUS_DARK) {
                        float tc1CloudThreshold = sdrCloudDataSamples[1][i];
                        if (!Float.isNaN(tc1CloudThreshold)) {
                            float B2_ac = samples[varIndexes[SDR_L2_OFFSET + 1]][i];
                            float B3_ac = samples[varIndexes[SDR_L2_OFFSET + 2]][i];
                            float B4_ac = samples[varIndexes[SDR_L2_OFFSET + 3]][i];
                            float B8A_ac = samples[varIndexes[SDR_L2_OFFSET + 8]][i];
                            float B11_ac = samples[varIndexes[SDR_L2_OFFSET + 9]][i];
                            float B12_ac = samples[varIndexes[SDR_L2_OFFSET + 10]][i];
                            float tc1 = (float) (0.3029 * B2_ac + 0.2786 * B3_ac + 0.4733 * B4_ac + 0.5599 * B8A_ac + 0.508 * B11_ac + 0.1872 * B12_ac);
                            if (tc1 < tc1CloudThreshold) {
                                status = STATUS_CLOUD_SHADOW;
//                            } else {
//                                // we observe that the feature is stable over time, we assume it is clear
//                                status = STATUS_LAND;
                            }
//                            if (AbstractLcMosaicAlgorithm.maybeIsPixelPos(2078, 2856, i, tileSize)) {
//                                System.err.println("ix=" + (i%tileSize) + " iy=" + (i/tileSize) + " tc1=" + tc1 + " tc1CloudThreshold=" + tc1CloudThreshold + " status=" + status);
//                            }
//                        } else {
//                            // we do not have temporal information, we are lost
//                            status = STATUS_LAND
                        }
                    }

                } else if ((status == STATUS_LAND /* || status == STATUS_HAZE || status == STATUS_BRIGHT */)) {
                    float sdr8 = samples[varIndexes[temporalCloudBandIndex]][i];
                    float sdr8CloudThreshold = sdrCloudDataSamples[0][i];
                    float sdr8CloudShadowThreshold = sdrCloudDataSamples[1][i];
                    if (maybeIsPixelPos(9280, 4656, i, tileSize)
                            || maybeIsPixelPos(4400, 9251, i, tileSize)) {
                        System.err.println("ix=" + (i % tileSize) + " iy=" + (i / tileSize) + " status LAND sdrMean=" + sdr8 + " sdr8CloudThreshold=" + sdr8CloudThreshold);
                    }
                    status = temporalCloudCheck(sdr8, sdr8CloudThreshold);
                    // added for ARGI processing
                    if (status == STATUS_LAND) {
                        status = temporalCloudShadowCheck2(sdr8, sdr8CloudShadowThreshold);
                    }
// excluded for AGRI processing, to be re-included for MERIS etc.
//                } else if (status == STATUS_CLOUD_SHADOW) {
//                    float sdr8 = samples[varIndexes[temporalCloudBandIndex]][i];
//                    float sdr8CloudShadowThreshold = sdrCloudDataSamples[1][i];
//                    if (maybeIsPixelPos(9280, 4656, i, tileSize)
//                            || maybeIsPixelPos(4400, 9251, i, tileSize)) {
//                        System.err.println("ix=" + (i % tileSize) + " iy=" + (i / tileSize) + " status SHADOW sdrMean=" + sdr8 + " sdr8CloudShadowThreshold=" + sdr8CloudShadowThreshold);
//                    }
//                    status = temporalCloudShadowCheck(sdr8, sdr8CloudShadowThreshold);
                }
            }

            if (status == STATUS_LAND) {
                int landCount = (int) aggregatedSamples[STATUS_LAND][i];
                // If we haven't seen LAND so far,
                // but we had SNOW, WATER or CLOUD_SHADOW => clear SDRs
                if (landCount == 0) {
                    int snowCount = (int) aggregatedSamples[STATUS_SNOW][i];
                    int waterCount = (int) aggregatedSamples[STATUS_WATER][i];
                    int shadowCount = (int) aggregatedSamples[STATUS_CLOUD_SHADOW][i];
                    if (snowCount > 0 || waterCount > 0 || shadowCount > 0) {
                        clearSDR(i, sensorConfig.getBandNames().length,0.0f);
                    }
                    aggregatedSamples[STATUS_BAND_INDEX][i] = STATUS_LAND;
                }
                // Since we have seen LAND now, accumulate LAND SDRs
                if (! bestPixelAggregation) {
                    addSdrs(samples, i);
                } else {
                    selectSdrs(samples, i, landCount);
                }
                // Count LAND
                aggregatedSamples[STATUS_LAND][i] = landCount + 1;
            } else if (status == STATUS_SNOW) {
                int landCount = (int) aggregatedSamples[STATUS_LAND][i];
                // If we haven't seen LAND so far, accumulate SNOW SDRs
                if (landCount == 0) {
                    // if there have been WATER or CLOUD_SHADOW  before => clear SDR
                    int waterCount = (int) aggregatedSamples[STATUS_WATER][i];
                    int shadowCount = (int) aggregatedSamples[STATUS_CLOUD_SHADOW][i];
                    if (waterCount > 0 || shadowCount > 0) {
                        clearSDR(i, sensorConfig.getBandNames().length, 0.0f);
                    }
                    if (! bestPixelAggregation) {
                        addSdrs(samples, i);
                    } else {
                        selectSdrs(samples, i, (int) aggregatedSamples[STATUS_SNOW][i]);
                    }
                    aggregatedSamples[STATUS_BAND_INDEX][i] = STATUS_SNOW;
                }
                // Count SNOW
                aggregatedSamples[STATUS_SNOW][i]++;
            } else if (status == STATUS_WATER) {
                if (Float.isNaN(sdr_1)) {
                    // deep water
                    deepWaterCounter[i]++;
                    aggregatedSamples[STATUS_BAND_INDEX][i] = STATUS_WATER;
                } else {
                    // shallow water
                    // only aggregate if, no LAND or SNOW before
                    int landCount = (int) aggregatedSamples[STATUS_LAND][i];
                    int snowCount = (int) aggregatedSamples[STATUS_SNOW][i];
                    if (landCount == 0 && snowCount == 0) {
                        // if there was a CLOUD_SHADOW before => clear SDR
                        int shadowCount = (int) aggregatedSamples[STATUS_CLOUD_SHADOW][i];
                        if (shadowCount > 0) {
                            clearSDR(i, sensorConfig.getBandNames().length, 0.0f);
                        }
                        if (! bestPixelAggregation) {
                            addSdrs(samples, i);
                        } else {
                            selectSdrs(samples, i, (int) aggregatedSamples[STATUS_WATER][i]);
                        }
                        aggregatedSamples[STATUS_BAND_INDEX][i] = STATUS_WATER;
                    }
                    aggregatedSamples[STATUS_WATER][i]++;
                }
            } else if (status == STATUS_CLOUD_SHADOW) {
                // Count CLOUD_SHADOW
                int landCount = (int) aggregatedSamples[STATUS_LAND][i];
                int snowCount = (int) aggregatedSamples[STATUS_SNOW][i];
                int waterCount = (int) aggregatedSamples[STATUS_WATER][i];
                if (landCount == 0 && snowCount == 0 && waterCount == 0) {
                    // only aggregate SDR, if no LAND, WATER or SNOW have been aggregated before
                    if (status != oldStatus) {
                        clearSDR(i, sensorConfig.getBandNames().length, 0.0f);
                        aggregatedSamples[STATUS_CLOUD_SHADOW][i] = 0;
                    }
                    if (! bestPixelAggregation) {
                        addSdrs(samples, i);
                    } else {
                        selectSdrs(samples, i, (int) aggregatedSamples[STATUS_CLOUD_SHADOW][i]);
                    }
                    aggregatedSamples[STATUS_BAND_INDEX][i] = STATUS_CLOUD_SHADOW;
                }
                aggregatedSamples[STATUS_CLOUD_SHADOW][i]++;
            } else if (status == STATUS_CLOUD) {
                // Count CLOUD
                aggregatedSamples[STATUS_CLOUD][i]++;
                if (oldStatus != STATUS_LAND &&
                        oldStatus != STATUS_SNOW &&
                        oldStatus != STATUS_WATER &&
                        oldStatus != STATUS_CLOUD_SHADOW &&
                        oldStatus != STATUS_BRIGHT &&
                        oldStatus != STATUS_DARK &&
                        oldStatus != STATUS_HAZE &&
                        oldStatus != STATUS_TEMPORAL_CLOUD) {
                    aggregatedSamples[STATUS_BAND_INDEX][i] = STATUS_CLOUD;
                }
            } else if (status == STATUS_TEMPORAL_CLOUD) {
                // Count CLOUD
                aggregatedSamples[STATUS_CLOUD][i]++;
                if (oldStatus != STATUS_LAND &&
                        oldStatus != STATUS_SNOW &&
                        oldStatus != STATUS_WATER &&
                        oldStatus != STATUS_CLOUD_SHADOW &&
                        oldStatus != STATUS_BRIGHT &&
                        oldStatus != STATUS_DARK &&
                        oldStatus != STATUS_HAZE) {
                    aggregatedSamples[STATUS_BAND_INDEX][i] = STATUS_TEMPORAL_CLOUD;
                }
            } else if (status == STATUS_BRIGHT || status == STATUS_DARK || status == STATUS_HAZE) {
                if (oldStatus != STATUS_LAND &&
                        oldStatus != STATUS_SNOW &&
                        oldStatus != STATUS_WATER &&
                        oldStatus != STATUS_CLOUD_SHADOW &&
                        (status == STATUS_BRIGHT ||
                                (status == STATUS_DARK && oldStatus != STATUS_BRIGHT) ||
                                (status == STATUS_HAZE && oldStatus != STATUS_BRIGHT && oldStatus != STATUS_DARK))) {
                    if (status != oldStatus) {
                        clearSDR(i, sensorConfig.getBandNames().length, 0.0f);
                        aggregatedSamples[STATUS_CLOUD_SHADOW][i] = 0;
                        aggregatedSamples[STATUS_BAND_INDEX][i] = status;
                    }
                    if (!bestPixelAggregation) {
                        addSdrs(samples, i);
                    } else {
                        selectSdrs(samples, i, (int) aggregatedSamples[STATUS_CLOUD_SHADOW][i]);
                    }
                    aggregatedSamples[STATUS_CLOUD_SHADOW][i]++;  // TODO: cloud shadow count abused for bright, dark, or haze
                }
            }
        }
    }

    protected int temporalCloudCheck(float sdr8, float sdr8CloudThreshold) {
        // if "ndvi" instead of sdr_B3 (spot only)
        if (!Float.isNaN(sdr8CloudThreshold) && sdr8 > sdr8CloudThreshold) {
            // treat this as cloud
            return STATUS_TEMPORAL_CLOUD;
        } else {
            return STATUS_LAND;
        }
    }

    protected int temporalCloudShadowCheck(float sdr8, float sdr8CloudShadowThreshold) {
        // if "ndvi" instead of sdr_B3 (spot only)
        if (!Float.isNaN(sdr8CloudShadowThreshold) && sdr8 > sdr8CloudShadowThreshold) {
            // treat this as cloud shadow
            return STATUS_LAND;
        } else {
            return STATUS_CLOUD_SHADOW;
        }
    }

    protected int temporalCloudShadowCheck2(float sdr8, float sdr8CloudShadowThreshold) {
        // if "ndvi" instead of sdr_B3 (spot only)
        if (!Float.isNaN(sdr8CloudShadowThreshold) && sdr8 < sdr8CloudShadowThreshold) {
            // treat this as cloud shadow
            return STATUS_CLOUD_SHADOW;
        } else {
            return STATUS_LAND;
        }
    }

    @Override
    public float[][] getTemporalResult() {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            int status = (int) aggregatedSamples[STATUS_BAND_INDEX][i];
            float wSum = 0f;
            if ((status == STATUS_LAND || status == STATUS_SNOW || status == STATUS_WATER || status == STATUS_CLOUD_SHADOW) && deepWaterCounter[i] == 0) {
                wSum = aggregatedSamples[status][i];
            } else if (status == STATUS_BRIGHT || status == STATUS_DARK || status == STATUS_HAZE) {
                wSum = aggregatedSamples[STATUS_CLOUD_SHADOW][i];
            }
            if (wSum != 0f) {
                for (int j = SDR_AGGREGATED_OFFSET; j < SDR_AGGREGATED_OFFSET + sensorConfig.getBandNames().length + 1; j++) {  // sdr + ndvi
                    aggregatedSamples[j][i] /= wSum;
                }
                for (int j = SDR_AGGREGATED_OFFSET + sensorConfig.getBandNames().length + 1; j < aggregatedSamples.length; j++) {  // sdr_error
                    aggregatedSamples[j][i] = ((float) Math.sqrt(aggregatedSamples[j][i])) / wSum;
                }
            } else {
                clearSDR(i, sensorConfig.getBandNames().length, Float.NaN);
            }
            if (deepWaterCounter[i] > 0) {
                aggregatedSamples[STATUS_WATER][i] = (int) aggregatedSamples[STATUS_WATER][i] + deepWaterCounter[i];
                aggregatedSamples[STATUS_BAND_INDEX][i] = STATUS_WATER;
            }
        }
        return aggregatedSamples;
    }

    @Override
    public String[] getTemporalFeatures() {
        return featureNames;
    }

    @Override
    public void setConf(Configuration jobConf) {
        this.jobConf = jobConf;
        if (sensorConfig == null) {
            String sensor = jobConf.get("calvalus.lc.sensor");
            String spatialResolution = jobConf.get("spatialResolution");
            sensorConfig = LcL3SensorConfig.create(sensor, spatialResolution);
            bestPixelAggregation = jobConf.getBoolean("calvalus.lc.bestpixelaggregation", false);
            System.out.println("calvalus.lc.bestpixelaggregation=" + bestPixelAggregation);
        }
    }

    protected void openSdr8MeanReader(int partition) throws IOException {
        String sdr8MeanDir = jobConf.get(CALVALUS_LC_SDR8_MEAN);
        NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);

        String fileName = "part-r-" + NUMBER_FORMAT.format(partition);
        Path path = new Path(sdr8MeanDir, fileName);
        FileSystem fs = path.getFileSystem(jobConf);
        reader = new SequenceFile.Reader(fs, path, jobConf);
        sdrCloudKey = new TileIndexWritable();
        sdrCloudData = new TileDataWritable();
    }

    @Override
    public Configuration getConf() {
        return jobConf;
    }

    protected void clearSDR(int i, int numSDRBands, float value) {
        for (int j = SDR_AGGREGATED_OFFSET; j < aggregatedSamples.length; j++) {  // sdr + ndvi + sdr_error
            aggregatedSamples[j][i] = value;
        }
    }

    protected void addSdrs(float[][] samples, int i) {
        final int numBands = sensorConfig.getBandNames().length;
        final boolean uncertaintiesAreSquares = sensorConfig.isUncertaintiesAreSquares();
        for (int j = SDR_AGGREGATED_OFFSET; j < SDR_AGGREGATED_OFFSET + numBands + 1; ++j) { // sdr + ndvi
            final int sdrJ = j - SDR_AGGREGATED_OFFSET + SDR_L2_OFFSET;
            aggregatedSamples[j][i] += samples[varIndexes[sdrJ]][i];
        }
        for (int j = SDR_AGGREGATED_OFFSET + numBands + 1; j < aggregatedSamples.length; ++j) {  // uncertainty
            final int sdrJ = j - SDR_AGGREGATED_OFFSET + SDR_L2_OFFSET;
            final float sdrErrorMeasurement = samples[varIndexes[sdrJ]][i];
            aggregatedSamples[j][i] += uncertaintiesAreSquares ? sdrErrorMeasurement : (sdrErrorMeasurement * sdrErrorMeasurement);
        }
    }

    protected void selectSdrs(float[][] samples, int i, int count) {
        if (count < 1) {
            addSdrs(samples, i);
        } else if (sensorConfig.isBetterPixel(samples, SDR_L2_OFFSET, aggregatedSamples, SDR_AGGREGATED_OFFSET, count, i)) {
            clearSDR(i, sensorConfig.getBandNames().length, 0.0f);
            addSdrs(samples, i);
            for (int j = SDR_AGGREGATED_OFFSET; j < aggregatedSamples.length; j++) {  // sdr + ndvi + sdr_error
                aggregatedSamples[j][i] *= count + 1;  // set weight
            }
        } else {
            for (int j = SDR_AGGREGATED_OFFSET; j < aggregatedSamples.length; j++) {  // sdr + ndvi + sdr_error
                aggregatedSamples[j][i] /= count;
                aggregatedSamples[j][i] *= count + 1;  // increase weight
            }
        }
    }

    static int calculateStatus(float land, float water, float snow, float cloud, float cloudShadow) {
        if (land > 0) {
            return STATUS_LAND;
        } else if (snow > 0) {
            return STATUS_SNOW;
        } else if (water > 0) {
            return STATUS_WATER;
        } else if (cloudShadow > 0) {
            return STATUS_CLOUD_SHADOW;
        } else if (cloud > 0) {
            return STATUS_CLOUD;
        } else {
            return STATUS_INVALID;
        }
    }

}
