/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l2tol3;

import org.esa.beam.binning.Observation;
import org.esa.beam.binning.VariableContext;
import org.esa.beam.binning.support.ObservationImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Calculates the ratio between a l3 mean and a l2 value
 */
public class RatioCalculator {

    private final int[] associatedMeanL3Index;
    private final Map<Long, float[]> l3MeanValues;

    public RatioCalculator(VariableContext variableContext, String[] l3MeanFeatureNames, Map<Long, float[]> l3MeanValues) {
        this.l3MeanValues = l3MeanValues;
        int variableCount = variableContext.getVariableCount();
        boolean[] isRatioVariable = getRatioVariables(variableContext, l3MeanFeatureNames);
        associatedMeanL3Index = new int[variableCount];

        ArrayList<String> l3BinNames = new ArrayList<>();
        Collections.addAll(l3BinNames, l3MeanFeatureNames);

        for (int i = 0; i < variableCount; i++) {
            if (isRatioVariable[i]) {
                String variableName = variableContext.getVariableName(i);
                associatedMeanL3Index[i] = l3BinNames.indexOf(variableName + "_mean");
            } else {
                associatedMeanL3Index[i] = -1;
            }
        }
    }

    public Observation calculateRatio(long l3BinIndex, Observation l2Observation) {
        float[] meanL3FeatureValues = l3MeanValues.get(l3BinIndex);
        int numObservations = l2Observation.size();
        float[] newObservationValues = new float[numObservations];
        for (int i = 0; i < numObservations; i++) {
            float l2Value = l2Observation.get(i);
            if (associatedMeanL3Index[i] != -1) {
                float meanValue = meanL3FeatureValues[associatedMeanL3Index[i]];
                newObservationValues[i] = l2Value / meanValue;
            } else {
                newObservationValues[i] = l2Value;
            }
        }
        return new ObservationImpl(l2Observation.getLatitude(),
                                   l2Observation.getLongitude(),
                                   l2Observation.getMJD(),
                                   newObservationValues);
    }

    static boolean[] getRatioVariables(VariableContext variableContext, String[] l3MeanFeatureNames) {
        List<String> l3Names = new ArrayList<>();
        Collections.addAll(l3Names, l3MeanFeatureNames);

        boolean[] isRatioVariable = new boolean[variableContext.getVariableCount()];
        for (int i = 0; i < isRatioVariable.length; i++) {
            String l2VariableName = variableContext.getVariableName(i);
            if (l3Names.contains(l2VariableName + "_mean")) {
                isRatioVariable[i] = true;
            }
        }
        return isRatioVariable;
    }
}
