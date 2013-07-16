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

package com.bc.calvalus.processing.mosaic.firecci;

import com.bc.calvalus.processing.mosaic.MosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.MosaicGrid;
import com.bc.calvalus.processing.mosaic.MosaicPartitioner;
import com.bc.calvalus.processing.mosaic.MosaicProductFactory;
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
public class FireMosaicAlgorithm implements MosaicAlgorithm, Configurable {

    public static final String CALVALUS_LC_SDR8_MEAN = "calvalus.lc.sdr8mean";

    private final static int[] SDR_BANDS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14};
    private final static int NUM_SDR_BANDS = SDR_BANDS.length;
    private static final int MINIMUM_BAND_INDEX = 13; // sdr_14
    private static final int SDR_CLOUD_BAND_INDEX = 8;

    private static final int STATUS_LAND = 1;
    private static final int STATUS_SNOW = 3;

    private static final int STATUS_CLOUD = 4;
    private String[] featureNames;
    private int[] varIndexes;
    private int tileSize;
    private int variableCount;

    private float[][] aggregatedSamples = null;

    private Configuration jobConf;
    private SequenceFile.Reader reader;
    private TileIndexWritable sdrCloudKey;
    private TileDataWritable sdrCloudData;
    private float[][] sdrCloudDataSamples;


    @Override
    public void setVariableContext(VariableContext variableContext) {
        featureNames = createOutputFeatureNames();
        varIndexes = createVariableIndexes(variableContext, featureNames);
        variableCount = featureNames.length;
        tileSize = MosaicGrid.create(jobConf).getTileSize();
    }

    @Override
    public void initTemporal(TileIndexWritable tileIndex) throws IOException {
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[variableCount][numElems];
        for (int band = 0; band < variableCount; band++) {
            Arrays.fill(aggregatedSamples[band], Float.NaN);
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

            if (status == STATUS_LAND && sdrCloudDataSamples != null) {
                status = temporalCloudCheck(samples[varIndexes[SDR_CLOUD_BAND_INDEX]][i], sdrCloudDataSamples[0][i]);
            }
            if (status == STATUS_LAND || status == STATUS_SNOW) {
                float aggregatedMin = aggregatedSamples[MINIMUM_BAND_INDEX][i];
                if (Float.isNaN(aggregatedMin) || samples[varIndexes[MINIMUM_BAND_INDEX]][i] < aggregatedMin) {
                    // set aggregated value to current measurement
                    for (final int varIndex : varIndexes) {
                        aggregatedSamples[varIndex][i] = samples[varIndex][i];
                    }
                }
            }
        }
    }

    protected int temporalCloudCheck(float sdr8, float sdr8CloudThreshold) {
        if (!Float.isNaN(sdr8CloudThreshold) && sdr8 > sdr8CloudThreshold) {
            // treat this as cloud
            return STATUS_CLOUD;
        } else {
            return STATUS_LAND;
        }
    }

    @Override
    public float[][] getTemporalResult() {
        return aggregatedSamples;
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
        return new FireMosaicProductFactory(getOutputFeatures());
    }

    @Override
    public void setConf(Configuration jobConf) {
        this.jobConf = jobConf;
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

    private static String[] createOutputFeatureNames() {
        String[] featureNames = new String[1 + NUM_SDR_BANDS + NUM_SDR_BANDS + 5];
        int j = 0;
        featureNames[j++] = "status";
        for (int i = 0; i < NUM_SDR_BANDS; i++) {
            featureNames[j++] = "sdr_" + SDR_BANDS[i];
        }
        for (int i = 0; i < NUM_SDR_BANDS; i++) {
            featureNames[j++] = "sdr_error_" + SDR_BANDS[i];
        }
        featureNames[j++] = "ndvi";
        featureNames[j++] = "sun_zenith";
        featureNames[j++] = "sun_azimuth";
        featureNames[j++] = "view_zenith";
        featureNames[j++] = "view_azimuth";
        return featureNames;
    }

    private static int[] createVariableIndexes(VariableContext varCtx, String[] outputFeatures) {
        int[] varIndexes = new int[outputFeatures.length];
        for (int i = 0; i < outputFeatures.length; i++) {
            varIndexes[i] = getVariableIndex(varCtx, outputFeatures[i]);
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

}
