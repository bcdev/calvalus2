package com.bc.calvalus.binning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Produces spatial bins by processing a given slice of observations.
 * A slice is referred to as a spatially contiguous region.
 *
 * @author Norman Fomferra
 */
public class SpatialBinner {

    private final BinningContext binningContext;
    private final BinningGrid binningGrid;
    private final BinManager binManager;
    private final SpatialBinProcessor processor;

    // State variables
    private final Map<Long, SpatialBin> activeBinMap;
    private final Map<Long, SpatialBin> finalizedBinMap;
    private final ArrayList<Exception> exceptions;

    /**
     * Constructs a spatial binner.
     *
     * @param binningContext       The binning context.
     * @param processor The processor that recieves the spatial bins processed from observations.
     */
    public SpatialBinner(BinningContext binningContext, SpatialBinProcessor processor) {
        this.binningContext = binningContext;
        this.binningGrid = binningContext.getBinningGrid();
        this.binManager = binningContext.getBinManager();
        this.processor = processor;
        this.activeBinMap = new HashMap<Long, SpatialBin>();
        this.finalizedBinMap = new HashMap<Long, SpatialBin>();
        this.exceptions = new ArrayList<Exception>();
    }

    /**
     * @return The binning context that will also be passed to {@link  SpatialBinProcessor#processSpatialBinSlice(BinningContext, java.util.List)}.
     */
    public BinningContext getBinningContext() {
        return binningContext;
    }

    /**
     * @return The exceptions occured during processing.
     */
    public Exception[] getExceptions() {
        return exceptions.toArray(new Exception[exceptions.size()]);
    }

    /**
     * Processes a slice of observations.
     * Will cause the {@link com.bc.calvalus.binning.SpatialBinProcessor} to be invoked.
     *
     * @param observations The observations.
     */
    public void processObservationSlice(Iterable<Observation> observations) {

        finalizedBinMap.putAll(activeBinMap);

        for (Observation observation : observations) {
            Long binIndex = binningGrid.getBinIndex(observation.getLatitude(), observation.getLongitude());
            SpatialBin bin = activeBinMap.get(binIndex);
            if (bin == null) {
                bin = binManager.createSpatialBin(binIndex);
                activeBinMap.put(binIndex, bin);
            }
            binManager.aggregateSpatialBin(observation, bin);
            finalizedBinMap.remove(binIndex);
        }

        if (!finalizedBinMap.isEmpty()) {
            emitSliceBins(finalizedBinMap);
            for (Long key : finalizedBinMap.keySet()) {
                activeBinMap.remove(key);
            }
            finalizedBinMap.clear();
        }
    }

    /**
     * Processes a slice of observations.
     * Convenience method for {@link #processObservationSlice(Iterable)}.
     *
     * @param observations The observations.
     */
    public void processObservationSlice(Observation... observations) {
        processObservationSlice(Arrays.asList(observations));
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
        finalizedBinMap.clear();
    }

    private void emitSliceBins(Map<Long, SpatialBin> binMap) {
        List<SpatialBin> bins = new ArrayList<SpatialBin>(binMap.values());
        for (SpatialBin bin : bins) {
            binManager.completeSpatialBin(bin);
        }
        try {
            processor.processSpatialBinSlice(binningContext, bins);
        } catch (Exception e) {
            exceptions.add(e);
        }
    }
}
