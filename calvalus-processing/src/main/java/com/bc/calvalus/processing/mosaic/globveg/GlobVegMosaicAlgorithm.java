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
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: marcoz
 * Date: 15.05.12
 * Time: 14:52
 * To change this template use File | Settings | File Templates.
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
    private static int VAR_VALID;

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
            int obsCount = 0;
            float obsTime = 0;
            float faparSum = 0.0f;
            float faparSumSqr = 0.0f;
            float laiSum = 0.0f;
            float laiSumSqr = 0.0f;

            for (float[][] sample : accu) {
                float valid = sample[varIndexes[VAR_VALID]][i];
                if (valid == 1f) {
                    float fapar = sample[varIndexes[VAR_FAPAR]][i];
                    float lai = sample[varIndexes[VAR_LAI]][i];
                    ++obsCount;
                    faparSum += fapar;
                    faparSumSqr += fapar * fapar;
                    laiSum += lai;
                    laiSumSqr += lai * lai;
                }
            }
            if (obsCount > 0) {
                final float faparMean = faparSum / obsCount;
                final float faparSigmaSqr = faparSumSqr / obsCount - faparMean * faparMean;
                final float faparSigma = faparSigmaSqr > 0.0f ? (float) Math.sqrt(faparSigmaSqr) : 0.0f;

                final float laiMean = laiSum / obsCount;
                final float laiSigmaSqr = laiSumSqr / obsCount - laiMean * laiMean;
                final float laiSigma = laiSigmaSqr > 0.0f ? (float) Math.sqrt(laiSigmaSqr) : 0.0f;

                float bestFapar = Float.NaN;
                float bestLai = Float.NaN;
                float bestFaparTime = Float.NaN;

                for (float[][] sample : accu) {
                    float valid = sample[varIndexes[VAR_VALID]][i];
                    if (valid == 1f) {
                        float time = sample[varIndexes[VAR_TIME]][i];
                        float fapar = sample[varIndexes[VAR_FAPAR]][i];
                        float lai = sample[varIndexes[VAR_LAI]][i];
                        if (Float.isNaN(bestFapar) || Math.abs(fapar - faparMean) < Math.abs(bestFapar - faparMean)) {
                            bestFapar = fapar;
                            bestLai = lai;
                            bestFaparTime = time;
                        }
                    }
                }
                aggregatedSamples[0][i] = obsCount;
                aggregatedSamples[1][i] = bestFaparTime;
                aggregatedSamples[2][i] = bestFapar;
                aggregatedSamples[3][i] = faparSigma;
                aggregatedSamples[4][i] = bestLai;
                aggregatedSamples[5][i] = laiSigma;
            } else {
                aggregatedSamples[0][i] = 0;
                aggregatedSamples[1][i] = Float.NaN;
                aggregatedSamples[2][i] = Float.NaN;
                aggregatedSamples[3][i] = Float.NaN;
                aggregatedSamples[4][i] = Float.NaN;
                aggregatedSamples[5][i] = Float.NaN;
            }
        }
        return aggregatedSamples;
    }

    @Override
    public void setVariableContext(VariableContext variableContext) {
        varIndexes = createVariableIndexes(variableContext);
        outputFeatures = createOutputFeatureNames();
        tileSize = MosaicGrid.create(jobConf).getTileSize();
    }

    private static String[] createOutputFeatureNames() {
        String[] featureNames = new String[6];
        int j = 0;
        featureNames[j++] = "obs_count";
        featureNames[j++] = "obs_time";
        featureNames[j++] = "fapar";
        featureNames[j++] = "fapar_sigma";
        featureNames[j++] = "lai";
        featureNames[j++] = "lai_sigma";
        return featureNames;
    }


    private static int[] createVariableIndexes(VariableContext varCtx) {
        int[] varIndexes = new int[4];
        int j = 0;
        varIndexes[VAR_VALID = j++] = getVariableIndex(varCtx, "valid");
        varIndexes[VAR_TIME = j++] = getVariableIndex(varCtx, "obs_time");
        varIndexes[VAR_FAPAR = j++] = getVariableIndex(varCtx, "FAPAR");
        varIndexes[VAR_LAI = j++] = getVariableIndex(varCtx, "LAI");
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
    private static class GlobVegMosaicProductFactory implements MosaicProductFactory {

        @Override
        public Product createProduct(String productName, Rectangle rect) {
            final Product product = new Product(productName, "CALVALUS-Mosaic", rect.width, rect.height);

            Band band = product.addBand("obs_count", ProductData.TYPE_INT8);
            band.setNoDataValue(0);
            band.setNoDataValueUsed(true);

            band = product.addBand("obs_time", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);

            band = product.addBand("fapar", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);

            band = product.addBand("fapar_sigma", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);

            band = product.addBand("lai", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);

            band = product.addBand("lai_sigma", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);

            //TODO
            //product.setStartTime(formatterConfig.getStartTime());
            //product.setEndTime(formatterConfig.getEndTime());
            return product;
        }
    }
}
