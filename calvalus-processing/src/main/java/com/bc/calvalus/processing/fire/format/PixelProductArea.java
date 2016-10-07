package com.bc.calvalus.processing.fire.format;

public class PixelProductArea {

    public int left;
    public int top;
    public int right;
    public int bottom;
    public int index;
    public String nicename;
    public String tiles;

    public PixelProductArea(int left, int top, int right, int bottom, int index, String nicename) {
        this(left, top, right, bottom, index, nicename, null);
    }

    public PixelProductArea(int left, int top, int right, int bottom, int index, String nicename, String tiles) {
        this.left = left;
        this.top = top;
        this.right = right;
        this.bottom = bottom;
        this.index = index;
        this.nicename = nicename;
        this.tiles = tiles;
    }
}
