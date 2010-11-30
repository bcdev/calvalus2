package com.bc.calvalus.b3;

import java.awt.image.Raster;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A default implementation of the {@link com.bc.calvalus.binning.Observation} interface.
 *
 * @author Norman Fomferra
 */
public class ObservationSlice implements Iterable<Observation> {
    private final Raster[] varRasters;
    private final ArrayList<Observation> observations;

    public ObservationSlice(Raster[] varRasters, int observationCapacity) {
        this.varRasters = varRasters;
        this.observations = new ArrayList<Observation>(observationCapacity);
    }

    public void addObservation(double lat, double lon, int x, int y) {
        final float[] samples = new float[varRasters.length];
        for (int i = 0; i < samples.length; i++) {
            samples[i] = varRasters[i].getSampleFloat(x, y, 0);
        }
        observations.add(new ObservationImpl(lat, lon, samples));
    }

    @Override
    public Iterator<Observation> iterator() {
        return observations.iterator();
    }
}
