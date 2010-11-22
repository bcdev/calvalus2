package com.bc.calvalus.binning;

class ObservationImpl implements Observation {
    double lat;
    double lon;

    double x;

    ObservationImpl(double lat, double lon) {
        this(lat, lon, 0.0);
    }

    ObservationImpl(double lat, double lon, double x) {
        this.lat = lat;
        this.lon = lon;
        this.x = x;
    }

    @Override
    public double getLat() {
        return lat;
    }

    @Override
    public double getLon() {
        return lon;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }
}
