package com.bc.calvalus.processing.fire.format.pixel.meris;

public enum PixelVariableType {

    DAY_OF_YEAR("JD"),
    CONFIDENCE_LEVEL("CL"),
    LC_CLASS("LC");

    final String bandName;

    PixelVariableType(String bandName) {
        this.bandName = bandName;
    }
}
