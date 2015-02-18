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
import com.bc.calvalus.processing.mosaic.MosaicProductFactory;
import org.apache.hadoop.conf.Configurable;

import java.lang.reflect.Array;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

/**
 * The algorithm, for lc_cci..
 *
 * @author MarcoZ
 */
public class LcL3Nc4MosaicAlgorithm extends AbstractLcMosaicAlgorithm {

    /* TODO: introduce additional band for second period
    final int VALID_COUNT_INDEX = 6;
     */
    static final String[] COUNTER_NAMES = { "clear_land", "clear_water", "clear_snow_ice", "cloud", "cloud_shadow" /*, "valid"*/ };

    protected String[] getCounterNames() { return COUNTER_NAMES; }

/*
    @Override
    public float[][] getTemporalResult() {
        super.getTemporalResult();
        int numElems = tileSize * tileSize;
        for (int i = 0; i < numElems; i++) {
            aggregatedSamples[VALID_COUNT_INDEX][i] =
                    aggregatedSamples[STATUS_LAND][i]
                            + aggregatedSamples[STATUS_WATER][i]
                            + aggregatedSamples[STATUS_SNOW][i]
                            + aggregatedSamples[STATUS_CLOUD][i]
                            + aggregatedSamples[STATUS_CLOUD_SHADOW][i];
        }
        return aggregatedSamples;
    }
*/

    /**
     * Removes bands 11 and 15 and substitutes sigma values by uncertainties (sqrt(sigma/numobs)).
     * @param temporalData  values of source bands and tile
     * @return  values of output bands and tile
     */
    @Override
    public float[][] getOutputResult(float[][] temporalData) {
        final List<String> temporalFeatures = Arrays.asList(getTemporalFeatures());
        final int currentPixelStateIndex = temporalFeatures.indexOf("status");
        final int clearLandCountIndex = temporalFeatures.indexOf("clear_land_count");
        final int clearSnowIceCountIndex = temporalFeatures.indexOf("clear_snow_ice_count");
        final int clearWaterCountIndex = temporalFeatures.indexOf("clear_water_count");
        final int cloudShadowCountIndex = temporalFeatures.indexOf("cloud_shadow_count");
        final int sr11MeanIndex = temporalFeatures.indexOf("sr_11_mean");
        final int sr15MeanIndex = temporalFeatures.indexOf("sr_15_mean");
        final int sr11SigmaIndex = temporalFeatures.indexOf("sr_11_sigma");
        final int sr15SigmaIndex = temporalFeatures.indexOf("sr_15_sigma");
        int targetLength = temporalData.length;
        if (sr11MeanIndex >= 0) {
            --targetLength;
        }
        if (sr15MeanIndex >= 0) {
            --targetLength;
        }
        if (sr11SigmaIndex >= 0) {
            --targetLength;
        }
        if (sr15SigmaIndex >= 0) {
            --targetLength;
        }
        final float[][] targetData = new float[targetLength][];
        int targetI = 0;
        for (int i=0; i<getTemporalFeatures().length; ++i) {
            if (i == sr11MeanIndex || i == sr15MeanIndex || i == sr11SigmaIndex || i == sr15SigmaIndex) {
                continue;
            }
            targetData[targetI++] = temporalData[i];
            if (getTemporalFeatures()[i].endsWith("_sigma")) {
                for (int j=0; j<temporalData[i].length; ++j) {
                    int currentPixelState = (int) temporalData[currentPixelStateIndex][j];
                    switch (currentPixelState) {
                        case 1:
                            temporalData[i][j] = (float) Math.sqrt(temporalData[i][j] / (int) temporalData[clearLandCountIndex][j]);
                            break;
                        case 2:
                            temporalData[i][j] = (float) Math.sqrt(temporalData[i][j] / (int) temporalData[clearWaterCountIndex][j]);
                            break;
                        case 3:
                            temporalData[i][j] = (float) Math.sqrt(temporalData[i][j] / (int) temporalData[clearSnowIceCountIndex][j]);
                            break;
                        case 5:
                            temporalData[i][j] = (float) Math.sqrt(temporalData[i][j] / (int) temporalData[cloudShadowCountIndex][j]);
                            break;
                        default:
                            temporalData[i][j] = Float.NaN;
                    }
                }
            }
        }
        return targetData;
    }

    /**
     * Removes bands 11 and 15 and replaces sigma by uncertainty bands.
     * @return
     */
    @Override
    public String[] getOutputFeatures() {
        final String[] temporalFeatures = getTemporalFeatures();
        final int sr11MeanIndex = Arrays.asList(getTemporalFeatures()).indexOf("sr_11_mean");
        final int sr15MeanIndex = Arrays.asList(getTemporalFeatures()).indexOf("sr_15_mean");
        final int sr11SigmaIndex = Arrays.asList(getTemporalFeatures()).indexOf("sr_11_sigma");
        final int sr15SigmaIndex = Arrays.asList(getTemporalFeatures()).indexOf("sr_15_sigma");
        int targetLength = temporalFeatures.length;
        if (sr11MeanIndex >= 0) {
            --targetLength;
        }
        if (sr15MeanIndex >= 0) {
            --targetLength;
        }
        if (sr11SigmaIndex >= 0) {
            --targetLength;
        }
        if (sr15SigmaIndex >= 0) {
            --targetLength;
        }
        final String[] outputFeatures = new String[targetLength];
        int targetI = 0;
        for (int i=0; i<temporalFeatures.length; ++i) {
            if (i == sr11MeanIndex || i == sr15MeanIndex || i == sr11SigmaIndex || i == sr15SigmaIndex) {
                //continue;
            } else if (temporalFeatures[i].equals("status")) {
                outputFeatures[targetI++] = "current_pixel_state";
            } else if (temporalFeatures[i].endsWith("_sigma")) {
                outputFeatures[targetI++] = temporalFeatures[i].substring(0, temporalFeatures[i].length()-"sigma".length()) + "uncertainty";
            } else {
                outputFeatures[targetI++] = temporalFeatures[i];
            }
        }
        return outputFeatures;
    }

    @Override
    public MosaicProductFactory getProductFactory() {
        return new LcL3Nc4MosaicProductFactory(getOutputFeatures());
    }


}
