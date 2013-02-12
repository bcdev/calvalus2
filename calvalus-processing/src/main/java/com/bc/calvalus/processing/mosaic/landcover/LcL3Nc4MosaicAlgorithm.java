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

/**
 * The algorithm, for lc_cci..
 *
 * @author MarcoZ
 */
public class LcL3Nc4MosaicAlgorithm extends AbstractLcMosaicAlgorithm implements MosaicAlgorithm, Configurable {

    /* TODO: introduce additional band for second period
    final int VALID_COUNT_INDEX = 6;
     */
    static final String[] COUNTER_NAMES = { "land", "water", "snow", "cloud", "cloud_shadow" /*, "valid"*/ };

    protected String[] getCounterNames() { return COUNTER_NAMES; }

/*
    @Override
    public float[][] getResult() {
        super.getResult();
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

    @Override
    public MosaicProductFactory getProductFactory() {
        return new LcL3Nc4MosaicProductFactory(getOutputFeatures());
    }


}
