/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.binning.*;
import com.bc.calvalus.binning.BinRasterizer;
import org.junit.Test;

import static org.junit.Assert.*;

public class L3ReprojectorTest {
    static final int NAN = -1;
    private BinManager binManager = new BinManagerImpl();

    @Test
    public void testPathExpansion() {
        assertEquals("part-r-00004", String.format("part-r-%05d", 4));
    }


    private TemporalBin createTBin(int idx) {
        TemporalBin temporalBin = binManager.createTemporalBin(idx);
        temporalBin.setNumObs(idx);
        return temporalBin;
    }

    private static class NobsRaster extends BinRasterizer {
        int[] nobsData;
        private final int w;

        private NobsRaster(int w, int h) {
            this.w = w;
            nobsData = new int[w * h];
        }

        @Override
        public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception {
            nobsData[y * w + x] = temporalBin.getNumObs();
        }

        @Override
        public void processMissingBin(int x, int y) throws Exception {
            nobsData[y * w + x] = -1;
        }
    }
}
