package com.bc.calvalus.processing.fire;

public enum ContinentalArea {
    northamerica(0, 7, 154, 71, "1", "North America"),
    southamerica(75, 71, 146, 147, "2", "South America"),
    europe(154, 7, 233, 65, "3", "Europe"),
    asia(233, 7, 360, 90, "4", "Asia"),
    africa(154, 65, 233, 130, "5", "Africa"),
    australia(275, 90, 360, 143, "6", "Australia"),
    greenland(130, 7, 154, 40, "7", "Greenland");

    final int left;
    final int top;
    final int right;
    final int bottom;
    final String index;
    final String nicename;

    ContinentalArea(int left, int top, int right, int bottom, String index, String nicename) {
        this.left = left;
        this.top = top;
        this.right = right;
        this.bottom = bottom;
        this.index = index;
        this.nicename = nicename;
    }
}
