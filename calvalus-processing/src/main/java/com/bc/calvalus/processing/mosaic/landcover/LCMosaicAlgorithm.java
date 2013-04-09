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

import com.bc.calvalus.processing.mosaic.MosaicProductFactory;

/**
 * The algorithm, for lc_cci..
 *
 * @author MarcoZ
 */
public class LCMosaicAlgorithm extends AbstractLcMosaicAlgorithm {

    static final String[] COUNTER_NAMES = { "land", "water", "snow", "cloud", "cloud_shadow" };

    protected String[] getCounterNames() { return COUNTER_NAMES; }

    @Override
    public float[][] getOutputResult(float[][] temporalData) {
        return temporalData;
    }

    @Override
    public String[] getOutputFeatures() {
        return getTemporalFeatures();
    }

    @Override
    public MosaicProductFactory getProductFactory() {
        return new LcMosaicProductFactory(getOutputFeatures());
    }
}
