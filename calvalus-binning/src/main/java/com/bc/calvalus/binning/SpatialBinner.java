package com.bc.calvalus.binning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Produces spatial bins for a given slice of observation.
 * A slice forms a spatially contiguous region.
 *
 * @param <OBS> The pixel type.
 * @param <BIN> The bin type.
 */
public class SpatialBinner<OBS extends Observation, BIN extends AbstractBin<OBS>> {

    private final BinningGrid grid;
    private final BinFactory<BIN> factory;
    private final BinConsumer<BIN> consumer;
    private final int numSlices;

    // State variables
    private int sliceIndex;
    private final Map<Integer, BIN> activeBinMap;
    private final Map<Integer, BIN> finalizedBinMap;

    public SpatialBinner(BinningGrid grid,
                         BinFactory<BIN> factory,
                         BinConsumer<BIN> consumer,
                         int numSlices) {
        this.grid = grid;
        this.factory = factory;
        this.consumer = consumer;
        this.numSlices = numSlices;
        this.activeBinMap = new HashMap<Integer, BIN>();
        this.finalizedBinMap = new HashMap<Integer, BIN>();
    }

    /**
     * Processes a slice of observations.
     * Will cause the {@link BinConsumer#consumeSlice(int, java.util.List) BinConsumer.consumeSlice()} method to be called.
     * @param observations The observations.
     */
    public void processSlice(OBS ... observations) {

        if (sliceIndex >= observations.length) {
            return;
        }

        finalizedBinMap.putAll(activeBinMap);

        for (OBS observation : observations) {
            Integer binIndex = grid.getBinIndex(observation.getLat(), observation.getLon());
            BIN bin = activeBinMap.get(binIndex);
            if (bin == null) {
                bin = factory.createBin(binIndex);
                activeBinMap.put(binIndex, bin);
            }
            bin.addObservation(observation);
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

    private void emitSliceBins(Map<Integer, BIN> binMap) {
        List<BIN> list = new ArrayList<BIN>(binMap.values());
        for (BIN bin : list) {
            bin.finish();
        }
        consumer.consumeSlice(sliceIndex, list);
    }


}
