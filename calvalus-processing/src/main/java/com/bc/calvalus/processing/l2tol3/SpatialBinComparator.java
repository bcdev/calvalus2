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

import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.Observation;
import org.esa.beam.binning.PlanetaryGrid;
import org.esa.beam.binning.SpatialBin;
import org.esa.beam.binning.SpatialBinConsumer;
import org.esa.beam.binning.SpatialBinner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Produces spatial bins by processing a given "slice" of observations.
 * A slice is referred to as a spatially contiguous region.
 * The class uses a {@link SpatialBinConsumer} to inform clients about a new slice of spatial bins ready to be consumed.
 *
 * @author Norman Fomferra
 */
public class SpatialBinComparator extends SpatialBinner {

    private final PlanetaryGrid planetaryGrid;
    private final BinManager binManager;
    private final SpatialBinConsumer consumer;

    // State variables
    private final Map<Integer, SpatialBin> activeBinMap;
    private final ArrayList<Exception> exceptions;
    private final int xAxisIndex;

    private final RatioCalculator ratioCalculator;

    /**
     * Constructs a spatial binner.
     *
     * @param binningContext The binning context.
     * @param consumer       The consumer that receives the spatial bins processed from observations.
     */
    public SpatialBinComparator(BinningContext binningContext, SpatialBinConsumer consumer, RatioCalculator ratioCalculator) {
        super(binningContext, consumer);
        this.ratioCalculator = ratioCalculator;
        this.planetaryGrid = binningContext.getPlanetaryGrid();
        this.binManager = binningContext.getBinManager();
        this.consumer = consumer;
        this.activeBinMap = new TreeMap<>();
        this.exceptions = new ArrayList<>();

        xAxisIndex = binningContext.getVariableContext().getVariableIndex("xaxis");
        if (xAxisIndex == -1) {
            throw new IllegalArgumentException("Variable 'xaxis' not defined.");
        }
    }

    /**
     * @return The exceptions occurred during processing.
     */
    public Exception[] getExceptions() {
        return exceptions.toArray(new Exception[exceptions.size()]);
    }

    /**
     * Processes a slice of observations.
     * Will cause the {@link SpatialBinConsumer} to be invoked.
     *
     * @param observations The observations.
     * @return The number of processed observations
     */
    public long processObservationSlice(Iterable<Observation> observations) {

        long observationCounter = 0;
        for (Observation observation : observations) {
            observationCounter++;
            Long l3BinIndex = planetaryGrid.getBinIndex(observation.getLatitude(), observation.getLongitude());
            float xaxisFloat = observation.get(xAxisIndex);
            int xAxisValue = (int) xaxisFloat;

            SpatialBin xAxisBin = activeBinMap.get(xAxisValue);
            if (xAxisBin == null) {
                xAxisBin = binManager.createSpatialBin(xAxisValue);
                activeBinMap.put(xAxisValue, xAxisBin);
            }
            Observation usedObservation = observation;
            if (ratioCalculator != null) {
                usedObservation = ratioCalculator.calculateRatio(l3BinIndex, observation);
            }
            binManager.aggregateSpatialBin(usedObservation, xAxisBin);
        }
        return observationCounter;
    }

    /**
     * Must be called after all observations have been send to {@link #processObservationSlice(Iterable)}.
     * Calling this method multiple times has no further effect.
     */
    public void complete() {
        if (!activeBinMap.isEmpty()) {
            emitSliceBins(activeBinMap);
            activeBinMap.clear();
        }
    }

    private void emitSliceBins(Map<Integer, SpatialBin> binMap) {
        List<SpatialBin> bins = new ArrayList<>(binMap.values());
        for (SpatialBin bin : bins) {
            binManager.completeSpatialBin(bin);
        }
        try {
            consumer.consumeSpatialBins(getBinningContext(), bins);
        } catch (Exception e) {
            exceptions.add(e);
        }
    }
}
