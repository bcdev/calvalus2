package com.bc.calvalus.b3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Produces spatial bins for a given slice of observation.
 * A slice forms a spatially contiguous region.
 *
 * @author Norman Fomferra
 */
public class SpatialBinner {

    private final BinningContext ctx;
    private final BinningGrid binningGrid;
    private final BinManager binManager;
    private final SpatialBinProcessor processor;
    private final int numSlices;

    // State variables
    private int sliceIndex;
    private final Map<Integer, SpatialBin> activeBinMap;
    private final Map<Integer, SpatialBin> finalizedBinMap;

    public SpatialBinner(BinningGrid binningGrid,
                         BinManager binManager,
                         SpatialBinProcessor processor,
                         int numSlices) {
        this(new BinningContextImpl(binningGrid, binManager), processor, numSlices);
    }

    public SpatialBinner(BinningContext ctx, SpatialBinProcessor processor, int numSlices) {
        this.ctx = ctx;
        this.binningGrid = ctx.getBinningGrid();
        this.binManager = ctx.getBinManager();
        this.processor = processor;
        this.numSlices = numSlices;
        this.activeBinMap = new HashMap<Integer, SpatialBin>();
        this.finalizedBinMap = new HashMap<Integer, SpatialBin>();
    }

    /**
     * Processes a slice of observations.
     * Will cause the {@link com.bc.calvalus.b3.SpatialBinProcessor} to be invoked.
     *
     * @param observations The observations.
     * @throws Exception if an error occurs
     */
    public void processObservationSlice(Observation... observations) throws Exception {

        if (sliceIndex >= observations.length) {
            return;
        }

        finalizedBinMap.putAll(activeBinMap);

        for (Observation observation : observations) {
            Integer binIndex = binningGrid.getBinIndex(observation.getLatitude(), observation.getLongitude());
            SpatialBin bin = activeBinMap.get(binIndex);
            if (bin == null) {
                bin = binManager.createSpatialBin(binIndex);
                activeBinMap.put(binIndex, bin);
            }
            binManager.aggregateSpatialBin(observation, bin);
            finalizedBinMap.remove(binIndex);
        }

        if (sliceIndex == numSlices - 1) {
            emitSliceBins(activeBinMap);
        } else if (sliceIndex > 0) {
            emitSliceBins(finalizedBinMap);
            for (Integer key : finalizedBinMap.keySet()) {
                activeBinMap.remove(key);
            }
            finalizedBinMap.clear();
        }

        sliceIndex++;
    }

    private void emitSliceBins(Map<Integer, SpatialBin> binMap) throws Exception {
        List<SpatialBin> list = new ArrayList<SpatialBin>(binMap.values());
        for (SpatialBin bin : list) {
            binManager.completeSpatialBin(bin);
        }
        processor.processSpatialBinSlice(ctx, sliceIndex, list);
    }


}
