/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic.globveg;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.mosaic.MosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.MosaicGrid;
import com.bc.calvalus.processing.mosaic.MosaicProductFactory;
import com.bc.calvalus.processing.mosaic.TileIndexWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.VariableContext;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.math.MathUtils;

import java.awt.Rectangle;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Composting using the algorithm described by Pinty et. al.
 */
public class GlobVegMosaicAlgorithm implements MosaicAlgorithm, Configurable {
    private String[] outputFeatures;
    private int tileSize;
    private Configuration jobConf;
    private int[] varIndexes;
    private List<float[][]> accu;

    private static int VAR_TIME;
    private static int VAR_FAPAR;
    private static int VAR_LAI;
    private static int VAR_VALID_FAPAR;
    private static int VAR_VALID_LAI;

    @Override
    public void init(TileIndexWritable tileIndex) {
        accu = new ArrayList<float[][]>();
    }

    @Override
    public void process(float[][] samples) {
        float[][] mySamples = new float[samples.length][];
        for (int i = 0; i < samples.length; i++) {
            mySamples[i] = samples[i].clone();
        }
        accu.add(mySamples);
    }

    @Override
    public float[][] getResult() {
        int numElems = tileSize * tileSize;
        float[][] aggregatedSamples = new float[outputFeatures.length][numElems];
        for (int i = 0; i < numElems; i++) {
            processSinglePixel(i, varIndexes[VAR_VALID_FAPAR], varIndexes[VAR_TIME], varIndexes[VAR_FAPAR], 0, aggregatedSamples, -1.0f / 65534.0f);
            processSinglePixel(i, varIndexes[VAR_VALID_LAI], varIndexes[VAR_TIME], varIndexes[VAR_LAI], 4, aggregatedSamples, -1.0f / (256 * 16 - 2));
        }
        return aggregatedSamples;
    }

    private void processSinglePixel(int i, int validIndex, int timeIndex, int measureIndex, int offset, float[][] aggregatedSamples, float noDataValue) {
        int obsCount = 0;
        double sum = 0.0f;
        double sumSqr = 0.0f;

        for (float[][] sample : accu) {
            float valid = sample[varIndexes[validIndex]][i];
            if (valid == 1f) {
                float measurement = sample[varIndexes[measureIndex]][i];
                ++obsCount;
                sum += measurement;
                sumSqr += measurement * measurement;
            }
        }
        if (obsCount > 0) {
            final float mean = (float) (sum / obsCount);
            final float sigmaSqr = (float) (sumSqr / obsCount - mean * mean);
            final float sigma = sigmaSqr > 0.0f ? (float) Math.sqrt(sigmaSqr) : 0.0f;

            float bestMeasurement = Float.NaN;
            float bestTime = Float.NaN;

            for (float[][] sample : accu) {
                final float valid = sample[varIndexes[validIndex]][i];
                if (valid == 1f) {
                    final float time = sample[varIndexes[timeIndex]][i];
                    final float measurement = sample[varIndexes[measureIndex]][i];
                    if (Float.isNaN(bestMeasurement)) {
                        bestMeasurement = measurement;
                        bestTime = time;
                    } else {
                        final float currentDistance = Math.abs(measurement - mean);
                        final float bestDistance = Math.abs(bestMeasurement - mean);

                        if (currentDistance < (bestDistance - 1E-6f) ||
                                (MathUtils.equalValues(currentDistance, bestDistance, 1E-6f) && measurement > bestMeasurement) || // same distance, but larger value
                                (measurement == bestMeasurement && time < bestTime) // same value, but earlier
                                ) {
                            bestMeasurement = measurement;
                            bestTime = time;
                        }
                    }
                }
            }
            aggregatedSamples[offset + 0][i] = obsCount;
            aggregatedSamples[offset + 1][i] = bestTime;
            aggregatedSamples[offset + 2][i] = bestMeasurement;
            aggregatedSamples[offset + 3][i] = sigma;
        } else {
            aggregatedSamples[offset + 0][i] = 0;
            aggregatedSamples[offset + 1][i] = Float.NaN;
            aggregatedSamples[offset + 2][i] = noDataValue;
            aggregatedSamples[offset + 3][i] = noDataValue;
        }
    }

    @Override
    public void setVariableContext(VariableContext variableContext) {
        varIndexes = createVariableIndexes(variableContext);
        outputFeatures = createOutputFeatureNames();
        tileSize = MosaicGrid.create(jobConf).getTileSize();
    }

    private static String[] createOutputFeatureNames() {
        String[] featureNames = new String[8];
        int j = 0;
        featureNames[j++] = "fapar_obs_count";
        featureNames[j++] = "fapar_obs_time";
        featureNames[j++] = "fapar";
        featureNames[j++] = "fapar_sigma";
        featureNames[j++] = "lai_obs_count";
        featureNames[j++] = "lai_obs_time";
        featureNames[j++] = "lai";
        featureNames[j++] = "lai_sigma";
        return featureNames;
    }


    private static int[] createVariableIndexes(VariableContext varCtx) {
        int[] varIndexes = new int[5];
        int j = 0;
        varIndexes[VAR_VALID_FAPAR = j++] = getVariableIndex(varCtx, "valid_fapar");
        varIndexes[VAR_VALID_LAI = j++] = getVariableIndex(varCtx, "valid_lai");
        varIndexes[VAR_TIME = j++] = getVariableIndex(varCtx, "obs_time");
        varIndexes[VAR_FAPAR = j++] = getVariableIndex(varCtx, "fapar");
        varIndexes[VAR_LAI = j++] = getVariableIndex(varCtx, "lai");
        return varIndexes;
    }

    private static int getVariableIndex(VariableContext varCtx, String varName) {
        int varIndex = varCtx.getVariableIndex(varName);
        if (varIndex < 0) {
            throw new IllegalArgumentException(String.format("varIndex < 0 for varName='%s'", varName));
        }
        return varIndex;
    }


    @Override
    public String[] getOutputFeatures() {
        return outputFeatures;
    }

    @Override
    public void setConf(Configuration conf) {
        this.jobConf = conf;
    }

    @Override
    public Configuration getConf() {
        return jobConf;
    }

    @Override
    public MosaicProductFactory getProductFactory() {
        return new GlobVegMosaicProductFactory();
    }

    /**
     * The factory for creating the final mosaic product for GlobVeg
     *
     * @author MarcoZ
     */
    private class GlobVegMosaicProductFactory implements MosaicProductFactory {

        @Override
        public Product createProduct(String productName, Rectangle rect) {
            final Product product = new Product(productName, "CALVALUS-Mosaic", rect.width, rect.height);

            Band band = product.addBand("fapar_obs_count", ProductData.TYPE_INT8);
            band.setNoDataValue(0);
            band.setNoDataValueUsed(true);

            band = product.addBand("fapar_obs_time", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);

            band = product.addBand("fapar", ProductData.TYPE_UINT16);
            band.setNoDataValue(0.0);
            band.setScalingFactor(1.0 / 65534.0);
            band.setScalingOffset(-1.0 / 65534.0);
            band.setNoDataValueUsed(true);

            band = product.addBand("fapar_sigma", ProductData.TYPE_UINT16);
            band.setNoDataValue(0.0);
            band.setScalingFactor(1.0 / 65534.0);
            band.setScalingOffset(-1.0 / 65534.0);
            band.setNoDataValueUsed(true);

            band = product.addBand("lai_obs_count", ProductData.TYPE_INT8);
            band.setNoDataValue(0);
            band.setNoDataValueUsed(true);

            band = product.addBand("lai_obs_time", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);

            band = product.addBand("lai", ProductData.TYPE_UINT16);
            band.setNoDataValue(0.0);
            band.setScalingFactor(1.0 / (256 * 16 - 2));
            band.setScalingOffset(-1.0 / (256 * 16 - 2));
            band.setNoDataValueUsed(true);

            band = product.addBand("lai_sigma", ProductData.TYPE_UINT16);
            band.setNoDataValue(0.0);
            band.setScalingFactor(1.0 / (256 * 16 - 2));
            band.setScalingOffset(-1.0 / (256 * 16 - 2));
            band.setNoDataValueUsed(true);

            String dateStart = jobConf.get(JobConfigNames.CALVALUS_MIN_DATE);
            if (dateStart != null) {
                product.setStartTime(parseTime(dateStart));
            }
            String dateStop = jobConf.get(JobConfigNames.CALVALUS_MAX_DATE);
            if (dateStop != null) {
                ProductData.UTC stopTime = parseTime(dateStop);
                Calendar calendar = stopTime.getAsCalendar();
                calendar.add(Calendar.DAY_OF_MONTH, 1);
                ProductData.UTC endTime = ProductData.UTC.create(calendar.getTime(), 0L);
                product.setEndTime(endTime);
            }

            return product;
        }

        private ProductData.UTC parseTime(String timeString) {
            try {
                return ProductData.UTC.parse(timeString, "yyyy-MM-dd");
            } catch (ParseException e) {
                throw new IllegalArgumentException("Illegal date format.", e);
            }
        }

    }
}
