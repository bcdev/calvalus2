package com.bc.calvalus.b3;

/**
 * A default implementation of the {@link Observation} interface.
 *
 * @author Norman Fomferra
 */
public final class ObservationImpl implements Observation {
    private final double latitude;
    private final double longitude;
    private final float[] measurements;

    public ObservationImpl(double latitude, double longitude) {
        this(latitude, longitude, 0.0f);
    }

    public ObservationImpl(double latitude, double longitude, float measurement) {
        this(latitude, longitude, new float[]{measurement});
    }

    public ObservationImpl(double latitude, double longitude, float[] measurements) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.measurements = measurements;
    }

    @Override
    public double getMJD() {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public double getLatitude() {
        return latitude;
    }

    @Override
    public double getLongitude() {
        return longitude;
    }

    @Override
    public int size() {
        return measurements.length;
    }

    @Override
    public float get(int varIndex) {
        return measurements[varIndex];
    }
}
