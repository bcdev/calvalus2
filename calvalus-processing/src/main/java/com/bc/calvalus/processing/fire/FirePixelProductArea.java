package com.bc.calvalus.processing.fire;

public enum FirePixelProductArea {

    NORTH_AMERICA(0, 7, 130, 71, 1),
    SOUTH_AMERICA(75, 71, 146, 147, 2),
    EUROPE(154, 7, 233, 65, 3),
    ASIA(233, 7, 360, 90, 4),
    AFRICA(154, 65, 233, 130, 5),
    AUSTRALIA(275, 90, 360, 143, 6);

    final int left;
    final int top;
    final int right;
    final int bottom;
    final int index;

    FirePixelProductArea(int left, int top, int right, int bottom, int index) {
        this.left = left;
        this.top = top;
        this.right = right;
        this.bottom = bottom;
        this.index = index;
    }
}
