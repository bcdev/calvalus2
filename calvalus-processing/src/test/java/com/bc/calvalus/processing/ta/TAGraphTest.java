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

package com.bc.calvalus.processing.ta;

import org.jfree.data.general.DatasetUtilities;
import org.jfree.data.xy.DefaultXYDataset;
import org.jfree.data.xy.XYDataset;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TAGraphTest {

    @Test
    public void testGetRegressionWithSigma() throws Exception {
        double[][] data = {
                {-3.20, 4.49, -1.66, 0.64, -2.43, -0.89, -0.12, 1.41, 2.95, 2.18, 3.72, 5.26},     // X
                {-7.14, -1.30, -4.26, -1.90, -6.19, -3.98, -2.87, -1.66, -0.78, -2.61, 0.31, 1.74} // Y
        };

        DefaultXYDataset xyDataset = new DefaultXYDataset();
        xyDataset.addSeries("", data);
        double[] coeffs = TAGraph.getRegressionWithSigma(xyDataset, 0);

        assertNotNull(coeffs);
        assertEquals(4, coeffs.length);
        assertEquals(-3.44596, coeffs[0], 1E-6); // a
        assertEquals(0.867329, coeffs[1], 1E-6); // b
        assertEquals(0.282225, coeffs[2], 1E-6); // stdev a
        assertEquals(0.099150, coeffs[3], 1E-6); // stdev b
    }

    @Test
    public void testGetMinMax() throws Exception {
        double[][] data = {
                {-3.20, 4.49, -1.66, 0.64, -2.43, -0.89, -0.12, 1.41, 2.95, 2.18, 3.72, 5.26},     // X
                {-7.14, -1.30, -4.26, -1.90, -6.19, -3.98, -2.87, -1.66, -0.78, -2.61, 0.31, 1.74} // Y
        };

        DefaultXYDataset xyDataset = new DefaultXYDataset();
        xyDataset.addSeries("", data);

        assertEquals(1.74, DatasetUtilities.findMaximumRangeValue(xyDataset).doubleValue(), 1E-6);
        assertEquals(-7.14, DatasetUtilities.findMinimumRangeValue(xyDataset).doubleValue(), 1E-6);

        XYDataset minMax = TAGraph.getMinMax(xyDataset, 0, 2);
        assertNotNull(minMax);
        assertEquals(12, minMax.getItemCount(0));
        assertEquals(2.7237106448737287, DatasetUtilities.findMaximumRangeValue(minMax).doubleValue(), 1E-6);
        assertEquals(-7.420430270367225, DatasetUtilities.findMinimumRangeValue(minMax).doubleValue(), 1E-6);

        minMax = TAGraph.getMinMax(xyDataset, 0, 1);
        assertNotNull(minMax);
        assertEquals(12, minMax.getItemCount(0));
        assertEquals(1.9199516236179388, DatasetUtilities.findMaximumRangeValue(minMax).doubleValue(), 1E-6);
        assertEquals(-6.820921995237573, DatasetUtilities.findMinimumRangeValue(minMax).doubleValue(), 1E-6);
    }

    @Test
    public void testGetMinMax_Simple() throws Exception {
        double[][] data = {
                {0, 1, 2}, // X
                {1, 4, 7}  // Y
        };

        DefaultXYDataset xyDataset = new DefaultXYDataset();
        xyDataset.addSeries("", data);

        double[] coeffs = TAGraph.getRegressionWithSigma(xyDataset, 0);

        assertNotNull(coeffs);
        assertEquals(4, coeffs.length);
        assertEquals(1.0, coeffs[0], 1E-6); // a
        assertEquals(3.0, coeffs[1], 1E-6); // b
        assertEquals(0.0, coeffs[2], 1E-6); // stdev a
        assertEquals(0.0, coeffs[3], 1E-6); // stdev b

        assertEquals(7.0, DatasetUtilities.findMaximumRangeValue(xyDataset).doubleValue(), 1E-6);
        assertEquals(1.0, DatasetUtilities.findMinimumRangeValue(xyDataset).doubleValue(), 1E-6);

        XYDataset minMax = TAGraph.getMinMax(xyDataset, 0, 2);
        assertNotNull(minMax);
        assertEquals(3, minMax.getItemCount(0));
        assertEquals(7.0, DatasetUtilities.findMaximumRangeValue(minMax).doubleValue(), 1E-6);
        assertEquals(1.0, DatasetUtilities.findMinimumRangeValue(minMax).doubleValue(), 1E-6);

        minMax = TAGraph.getMinMax(xyDataset, 0, 1);
        assertNotNull(minMax);
        assertEquals(3, minMax.getItemCount(0));
        assertEquals(7.0, DatasetUtilities.findMaximumRangeValue(minMax).doubleValue(), 1E-6);
        assertEquals(1.0, DatasetUtilities.findMinimumRangeValue(minMax).doubleValue(), 1E-6);
    }

    @Test
    public void testGetMinMax_Simple2() throws Exception {
        double[][] data = {
                {0, 1, 2, 3, 4}, // X
                {1, 4, 7, 4, 2}  // Y
        };

        DefaultXYDataset xyDataset = new DefaultXYDataset();
        xyDataset.addSeries("", data);

        double[] coeffs = TAGraph.getRegressionWithSigma(xyDataset, 0);

        assertNotNull(coeffs);
        assertEquals(4, coeffs.length);
        assertEquals(3.2, coeffs[0], 1E-6); // a
        assertEquals(0.2, coeffs[1], 1E-6); // b
        assertEquals(2.039607805437114, coeffs[2], 1E-6); // stdev a
        assertEquals(0.8326663997864532, coeffs[3], 1E-6); // stdev b

        assertEquals(7.0, DatasetUtilities.findMaximumRangeValue(xyDataset).doubleValue(), 1E-6);
        assertEquals(1.0, DatasetUtilities.findMinimumRangeValue(xyDataset).doubleValue(), 1E-6);

        XYDataset minMax = TAGraph.getMinMax(xyDataset, 0, 2);
        assertNotNull(minMax);
        assertEquals(5, minMax.getItemCount(0));
        assertEquals(14.740546809165853, DatasetUtilities.findMaximumRangeValue(minMax).doubleValue(), 1E-6);
        assertEquals(-6.740546809165854, DatasetUtilities.findMinimumRangeValue(minMax).doubleValue(), 1E-6);

        minMax = TAGraph.getMinMax(xyDataset, 0, 1);
        assertNotNull(minMax);
        assertEquals(5, minMax.getItemCount(0));
        assertEquals(9.370273404582926, DatasetUtilities.findMaximumRangeValue(minMax).doubleValue(), 1E-6);
        assertEquals(-1.3702734045829268, DatasetUtilities.findMinimumRangeValue(minMax).doubleValue(), 1E-6);
    }
}
