package com.bc.calvalus.processing.fire.format;

public class PixelProductArea {

    public int left;
    public int top;
    public int right;
    public int bottom;
    public int index;
    public String nicename;

    PixelProductArea(int left, int top, int right, int bottom, int index, String nicename) {
        this.left = left;
        this.top = top;
        this.right = right;
        this.bottom = bottom;
        this.index = index;
        this.nicename = nicename;
    }
}
