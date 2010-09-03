package org.esa.beam.gpf.operators.matchup;

import org.esa.beam.framework.datamodel.GeoPos;

import java.util.Date;


public class ReferenceMeasurement {
    private final String id;
    private final Date time;
    private final GeoPos location;

    public ReferenceMeasurement(String id, Date time, GeoPos location) {
        this.id = id;
        this.time = time;
        this.location = location;
    }

    public String getId() {
        return id;
    }

    public Date getTime() {
        return time;
    }

    public GeoPos getLocation() {
        return location;
    }
}
