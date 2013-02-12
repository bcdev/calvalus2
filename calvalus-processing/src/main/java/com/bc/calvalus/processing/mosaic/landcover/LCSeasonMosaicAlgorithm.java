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
import org.esa.beam.binning.VariableContext;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.ColorPaletteDef;
import org.esa.beam.framework.datamodel.ImageInfo;
import org.esa.beam.framework.datamodel.IndexCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.awt.Color;
import java.awt.Rectangle;
import java.io.IOException;
import java.util.Arrays;

/**
 * The algorithm, for lc_cci..
 *
 * TODO adopt to SPOT-VGT products
 *
 * @author MarcoZ
 */
public class LCSeasonMosaicAlgorithm implements MosaicAlgorithm, Configurable {

    private static final int STATUS = 0;

    static final int STATUS_INVALID = 0;
    static final int STATUS_LAND = 1;
    static final int STATUS_WATER = 2;
    static final int STATUS_SNOW = 3;
    static final int STATUS_CLOUD = 4;
    static final int STATUS_CLOUD_SHADOW = 5;

    private static final String[] COUNTER_NAMES = {"land", "water", "snow", "cloud", "cloud_shadow"};

    private static final int SDR_OFFSET = COUNTER_NAMES.length + 1;
    private static final int NUM_SDR_BANDS = 15;
    private static final int NUM_BANDS = 1 + 5 + 15 + 1 + 15;

    private float[][] aggregatedSamples = null;
    private int tileSize;
    private Configuration jobConf;


    @Override
    public void init(TileIndexWritable tileIndex) throws IOException {
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[NUM_BANDS][numElems];
        for (int band = 0; band < NUM_BANDS; band++) {
            Arrays.fill(aggregatedSamples[band], 0.0f);
        }
    }

    @Override
    public void process(float[][] samples) {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            int sampleStatus = (int) samples[STATUS][i];
            int previousStatus = (int) aggregatedSamples[STATUS][i];

            if (sampleStatus == STATUS_LAND) {
                // If we haven't seen LAND so far, but we had SNOW or WATER, delete SDRs measurements
                if (previousStatus != STATUS_LAND) {
                    clearSDR(i, 0.0f);
                    aggregatedSamples[STATUS][i] = STATUS_LAND;
                }
                // Since we have seen LAND now, accumulate LAND SDRs
                final int sampleLandCount = (int) samples[STATUS_LAND][i];
                addSdrs(samples, sampleLandCount, i);
            } else if (sampleStatus == STATUS_SNOW) {
                // If we haven't seen LAND so far, accumulate SNOW SDRs
                if (previousStatus != STATUS_LAND && previousStatus != STATUS_SNOW) {
                    clearSDR(i, 0.0f);
                    aggregatedSamples[STATUS][i] = STATUS_SNOW;
                }
                if (previousStatus != STATUS_LAND) {
                    final int sampleSnowCount = (int) samples[STATUS_SNOW][i];
                    addSdrs(samples, sampleSnowCount, i);
                }
            } else if (sampleStatus == STATUS_WATER ) {
                if (previousStatus != STATUS_LAND && previousStatus != STATUS_SNOW && previousStatus != STATUS_WATER) {
                    clearSDR(i, 0.0f);
                    aggregatedSamples[STATUS][i] = STATUS_WATER;
                }
                if (previousStatus != STATUS_LAND && previousStatus != STATUS_SNOW) {
                    final int sampleWaterCount = (int) samples[STATUS_WATER][i];
                    addSdrs(samples, sampleWaterCount, i);
                }
            } else if (sampleStatus == STATUS_CLOUD || sampleStatus == STATUS_CLOUD_SHADOW) {
                if (previousStatus == STATUS_INVALID) {
                    aggregatedSamples[STATUS][i] = STATUS_CLOUD;
                }
            }
            for (int j = 0; j < COUNTER_NAMES.length; j++) {
                aggregatedSamples[j + 1][i] += samples[j + 1][i];
            }
        }
    }

    @Override
    public float[][] getResult() {
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            int status = (int) aggregatedSamples[STATUS][i];

            float wSum = 0f;
            if ((status == STATUS_LAND || status == STATUS_SNOW || status == STATUS_WATER)) {
                wSum = aggregatedSamples[status][i];
            }
            if (wSum != 0f) {
                for (int j = 0; j < NUM_SDR_BANDS + NUM_SDR_BANDS + 1; j++) {  // sdr + ndvi + sdr_error
                    aggregatedSamples[SDR_OFFSET + j][i] /= wSum;
                }
            } else {
                clearSDR(i, Float.NaN);
            }
        }
        return aggregatedSamples;
    }

    @Override
    public void setVariableContext(VariableContext variableContext) {
        tileSize = MosaicGrid.create(jobConf).getTileSize();
    }

    @Override
    public String[] getOutputFeatures() {
        return new String[0];
    }

    @Override
    public MosaicProductFactory getProductFactory() {
        return new LcMosaicProductFactory();
    }


    @Override
    public void setConf(Configuration jobConf) {
        this.jobConf = jobConf;
    }

    @Override
    public Configuration getConf() {
        return jobConf;
    }

    private void clearSDR(int i, float value) {
        for (int j = 0; j < NUM_SDR_BANDS + NUM_SDR_BANDS + 1; j++) {
            aggregatedSamples[SDR_OFFSET + j][i] = value;
        }
    }

    private void addSdrs(float[][] samples, int weightCount, int i) {
        int sdrOffset = SDR_OFFSET;
        for (int j = 0; j < NUM_SDR_BANDS + 1; j++) { // sdr + ndvi
            float sdrMeasurement =  samples[sdrOffset + j][i] * weightCount;
            aggregatedSamples[sdrOffset + j][i] += sdrMeasurement;
        }
        sdrOffset += NUM_SDR_BANDS + 1;
        for (int j = 0; j < NUM_SDR_BANDS; j++) { // sdr_error
            float sdrErrorMeasurement =  samples[sdrOffset + j][i] * weightCount;
            aggregatedSamples[sdrOffset + j][i] += (sdrErrorMeasurement * sdrErrorMeasurement);
        }
    }

    /**
     * The factory for creating the final mosaic product for LC-CCI
     *
     * @author MarcoZ
     */
    private static class LcMosaicProductFactory extends DefaultMosaicProductFactory {

        static final float[] WAVELENGTH = new float[]{
                412.691f, 442.55902f, 489.88202f, 509.81903f, 559.69403f,
                619.601f, 664.57306f, 680.82104f, 708.32904f, 753.37103f,
                761.50806f, 778.40906f, 864.87604f, 884.94403f, 900.00006f};

        @Override
        public Product createProduct(String productName, Rectangle rect) {
            final Product product = new Product(productName, "CALVALUS-Mosaic", rect.width, rect.height);

            Band band = product.addBand("status", ProductData.TYPE_INT8);
            band.setNoDataValue(0);
            band.setNoDataValueUsed(true);

            final IndexCoding indexCoding = new IndexCoding("status");
            ColorPaletteDef.Point[] points = new ColorPaletteDef.Point[5];
            indexCoding.addIndex("land", 1, "");
            points[0] = new ColorPaletteDef.Point(1, Color.GREEN, "land");
            indexCoding.addIndex("water", 2, "");
            points[1] = new ColorPaletteDef.Point(2, Color.BLUE, "water");
            indexCoding.addIndex("snow", 3, "");
            points[2] = new ColorPaletteDef.Point(3, Color.YELLOW, "snow");
            indexCoding.addIndex("cloud", 4, "");
            points[3] = new ColorPaletteDef.Point(4, Color.WHITE, "cloud");
            indexCoding.addIndex("cloud_shadow", 5, "");
            points[4] = new ColorPaletteDef.Point(5, Color.GRAY, "cloud_shadow");
            product.getIndexCodingGroup().add(indexCoding);
            band.setSampleCoding(indexCoding);
            band.setImageInfo(new ImageInfo(new ColorPaletteDef(points, points.length)));

            for (String counter : COUNTER_NAMES) {
                band = product.addBand(counter + "_count", ProductData.TYPE_INT8);
                band.setNoDataValue(-1);
                band.setNoDataValueUsed(true);
            }
            for (int i = 0; i < NUM_SDR_BANDS; i++) {
                int bandIndex = i + 1;
                band = product.addBand("sr_" + bandIndex + "_mean", ProductData.TYPE_FLOAT32);
                band.setNoDataValue(Float.NaN);
                band.setNoDataValueUsed(true);
                band.setSpectralBandIndex(bandIndex);
                band.setSpectralWavelength(WAVELENGTH[i]);
            }
            band = product.addBand("ndvi_mean", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
            for (int i = 0; i < NUM_SDR_BANDS; i++) {
                band = product.addBand("sr_" + (i + 1) + "_sigma", ProductData.TYPE_FLOAT32);
                band.setNoDataValue(Float.NaN);
                band.setNoDataValueUsed(true);
            }
            product.setAutoGrouping("mean:sigma:count");

            //TODO
            //product.setStartTime(formatterConfig.getStartTime());
            //product.setEndTime(formatterConfig.getEndTime());
            return product;
        }
    }
}
