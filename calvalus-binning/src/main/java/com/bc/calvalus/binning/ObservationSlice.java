package com.bc.calvalus.binning;

import java.awt.image.Raster;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A "slice" of observations. A slice is a spatially contiguous area of observations.
 *
 * @author Norman Fomferra
 */
public final class ObservationSlice implements Iterable<Observation> {
    private final Raster[] sourceTiles;
    private final ArrayList<Observation> observations;

    public ObservationSlice(Raster[] sourceTiles, int observationCapacity) {
        this.sourceTiles = sourceTiles;
        this.observations = new ArrayList<Observation>(observationCapacity);
    }

    public float[] createObservationSamples(int x, int y) {
        final float[] samples = new float[sourceTiles.length];
        for (int i = 0; i < samples.length; i++) {
            samples[i] = sourceTiles[i].getSampleFloat(x, y, 0);
        }
        return samples;
    }

    public void addObservation(double lat, double lon, float[] samples) {
        observations.add(new ObservationImpl(lat, lon, samples));
    }

    public int getSize() {
        return observations.size();
    }

    @Override
    public Iterator<Observation> iterator() {
        return observations.iterator();
    }
}
