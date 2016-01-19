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
import org.esa.beam.binning.VariableContext;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;

/**
 * The algorithm, for lc_cci..
 *
 * @author MarcoZ
 */
abstract public class AbstractLcMosaicAlgorithm implements MosaicAlgorithm, Configurable {

    protected final int SDR_L2_OFFSET = 1;
    protected final int SDR_AGGREGATED_OFFSET = getCounterNames().length + 1;
    abstract protected String[] getCounterNames();

    private LcL3SensorConfig sensorConfig = null;

    protected static final int STATUS = 0;

    static final int STATUS_INVALID = 0;
    static final int STATUS_LAND = 1;
    static final int STATUS_WATER = 2;
    static final int STATUS_SNOW = 3;
    static final int STATUS_CLOUD = 4;
    static final int STATUS_CLOUD_SHADOW = 5;

    public static final String CALVALUS_LC_SDR8_MEAN = "calvalus.lc.sdr8mean";

    protected int[] varIndexes;
    protected StatusRemapper statusRemapper;

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
    }

    @Override
    public void initTemporal(TileIndexWritable tileIndex) throws IOException {
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

    @Override
    public void processTemporal(float[][] samples) {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            int status = (int) samples[varIndexes[0]][i];

            //check for invalid pixels
            float sdr_1 = samples[varIndexes[SDR_L2_OFFSET]][i];
            if ((status == STATUS_LAND || status == STATUS_SNOW) && Float.isNaN(sdr_1)) {
                status = STATUS_INVALID;
            }
            status = StatusRemapper.remapStatus(statusRemapper, status);

            if (status == STATUS_LAND && sdrCloudDataSamples != null) {
                status = temporalCloudCheck(samples[varIndexes[sensorConfig.getTemporalCloudBandIndex()]][i], sdrCloudDataSamples[0][i]);
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
                }
                // Since we have seen LAND now, accumulate LAND SDRs
                addSdrs(samples, i);
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
                    addSdrs(samples, i);
                }
                // Count SNOW
                aggregatedSamples[STATUS_SNOW][i]++;
            } else if (status == STATUS_WATER ) {
                if (Float.isNaN(sdr_1)) {
                    // deep water
                    deepWaterCounter[i]++;
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
                        addSdrs(samples, i);
                    }
                    aggregatedSamples[STATUS_WATER][i]++;
                }
            } else if (status == STATUS_CLOUD) {
                // Count CLOUD
                aggregatedSamples[STATUS_CLOUD][i]++;
            } else if (status == STATUS_CLOUD_SHADOW) {
                // Count CLOUD_SHADOW
                int landCount = (int) aggregatedSamples[STATUS_LAND][i];
                int snowCount = (int) aggregatedSamples[STATUS_SNOW][i];
                int waterCount = (int) aggregatedSamples[STATUS_WATER][i];
                if (landCount == 0 && snowCount == 0 && waterCount == 0) {
                    // only aggregate SDR, if no LAND, WATER or SNOW have been aggregated before
                    addSdrs(samples, i);
                }
                aggregatedSamples[STATUS_CLOUD_SHADOW][i]++;
            }
        }
    }

    protected int temporalCloudCheck(float sdr8, float sdr8CloudThreshold) {
        // if "ndvi" instead of sdr_B3 (spot only)
        //if (!Float.isNaN(sdr8CloudThreshold) && sdr8 > sdr8CloudThreshold) {
        if (!Float.isNaN(sdr8CloudThreshold) && sdr8 > sdr8CloudThreshold) {
            // treat this as cloud
            return STATUS_CLOUD;
        } else {
            return STATUS_LAND;
        }
    }

    @Override
    public float[][] getTemporalResult() {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            int status = calculateStatus(aggregatedSamples[STATUS_LAND][i],
                                         aggregatedSamples[STATUS_WATER][i],
                                         aggregatedSamples[STATUS_SNOW][i],
                                         aggregatedSamples[STATUS_CLOUD][i],
                                         aggregatedSamples[STATUS_CLOUD_SHADOW][i]);

            aggregatedSamples[STATUS][i] = status;
            float wSum = 0f;
            if ((status == STATUS_LAND || status == STATUS_SNOW || status == STATUS_WATER || status == STATUS_CLOUD_SHADOW) && deepWaterCounter[i] == 0) {
                wSum = aggregatedSamples[status][i];
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
                aggregatedSamples[STATUS][i] = STATUS_WATER;
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
            sensorConfig = LcL3SensorConfig.create(jobConf.get("calvalus.lc.resolution"));
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
/*
        } else if (cloud > 0 || cloudShadow > 0) {  // TODO: Do not want to change it in the middle of processing but shadow is preferred over cloud now. Boe, 2015-02-22
            if (cloud > cloudShadow) {
                return STATUS_CLOUD;
            } else {
                return STATUS_CLOUD_SHADOW;
            }
*/
        } else {
            return STATUS_INVALID;
        }
    }

}
