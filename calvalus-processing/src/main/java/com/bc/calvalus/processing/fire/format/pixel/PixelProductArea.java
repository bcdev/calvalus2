package com.bc.calvalus.processing.fire.format.pixel;

public enum PixelProductArea {

    NORTH_AMERICA(0, 7, 130, 71, 1, "North America"),
    SOUTH_AMERICA(75, 71, 146, 147, 2, "South America"),
    EUROPE(154, 7, 233, 65, 3, "Europe"),
    ASIA(233, 7, 360, 90, 4, "Asia"),
    AFRICA(154, 65, 233, 130, 5, "Africa"),
    AUSTRALIA(275, 90, 360, 143, 6, "Australia");

    final int left;
    final int top;
    final int right;
    final int bottom;
    final int index;
    final String nicename;

    PixelProductArea(int left, int top, int right, int bottom, int index, String nicename) {
        this.left = left;
        this.top = top;
        this.right = right;
        this.bottom = bottom;
        this.index = index;
        this.nicename = nicename;
    }

}
