package com.bc.calvalus.processing.fire.format;

public class PixelProductArea {

    public int left;
    public int top;
    public int right;
    public int bottom;
    public String index;
    public String nicename;

    public PixelProductArea(int left, int top, int right, int bottom, String nicename) {
        this(left, top, right, bottom, "", nicename);
    }

    public PixelProductArea(int left, int top, int right, int bottom, String index, String nicename) {
        this.left = left;
        this.top = top;
        this.right = right;
        this.bottom = bottom;
        this.nicename = nicename;
        this.index = index;
    }
}
