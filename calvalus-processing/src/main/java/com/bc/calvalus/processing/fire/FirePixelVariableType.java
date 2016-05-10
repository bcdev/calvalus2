package com.bc.calvalus.processing.fire;

public enum FirePixelVariableType {

    DAY_OF_YEAR("JD"),
    CONFIDENCE_LEVEL("CL"),
    LC_CLASS("LC");

    final String bandName;

    FirePixelVariableType(String bandName) {
        this.bandName = bandName;
    }
}
