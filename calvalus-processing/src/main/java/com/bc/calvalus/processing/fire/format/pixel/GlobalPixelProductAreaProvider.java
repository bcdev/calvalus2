package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.processing.fire.format.PixelProductArea;
import com.bc.calvalus.processing.fire.format.SensorStrategy;

public class GlobalPixelProductAreaProvider implements SensorStrategy.PixelProductAreaProvider {

    enum GlobalPixelProductArea {
        NORTH_AMERICA(0, 7, 154, 71, "1", "North America"),
        SOUTH_AMERICA(75, 71, 146, 147, "2", "South America"),
        EUROPE(154, 7, 233, 65, "3", "Europe"),
        ASIA(233, 7, 360, 90, "4", "Asia"),
        AFRICA(154, 65, 233, 130, "5", "Africa"),
        AUSTRALIA(275, 90, 360, 143, "6", "Australia"),
        GREENLAND(130, 7, 154, 40, "7", "Greenland");

        final int left;
        final int top;
        final int right;
        final int bottom;
        final String index;
        final String nicename;

        GlobalPixelProductArea(int left, int top, int right, int bottom, String index, String nicename) {
            this.left = left;
            this.top = top;
            this.right = right;
            this.bottom = bottom;
            this.index = index;
            this.nicename = nicename;
        }
    }

    @Override
    public PixelProductArea getArea(String identifier) {
        return translate(GlobalPixelProductArea.valueOf(identifier));
    }

    private static PixelProductArea translate(GlobalPixelProductArea mppa) {
        return new PixelProductArea(mppa.left, mppa.top, mppa.right, mppa.bottom, mppa.index, mppa.nicename);
    }

    @Override
    public PixelProductArea[] getAllAreas() {
        PixelProductArea[] result = new PixelProductArea[GlobalPixelProductArea.values().length];
        GlobalPixelProductArea[] values = GlobalPixelProductArea.values();
        for (int i = 0; i < values.length; i++) {
            GlobalPixelProductArea pixelProductArea = values[i];
            result[i] = translate(pixelProductArea);
        }
        return result;
    }
}
