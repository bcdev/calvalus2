package com.bc.calvalus.experiments.matchup;

import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.ProductData;


public class MatchupDataset {

    private final GeoPos location;
    private final ProductData.UTC observationTime;
    private String name;

    public MatchupDataset(GeoPos location, ProductData.UTC observationTime) {
        this.location = location;
        this.observationTime = observationTime;
    }

    public void setProductName(String name) {
        this.name = name;
    }

    public void setBandData(String name, double sample, int sampleCount) {
        //TODO
    }

    public GeoPos getLocation() {
        return location;
    }

    public ProductData.UTC getObservationTime() {
        return observationTime;
    }

    public String getName() {
        return name;
    }
}
