package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.processing.fire.format.pixel.MerisPixelInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;

public class MerisStrategy implements SensorStrategy {

    private static final int RASTER_WIDTH = 3600;
    private static final int RASTER_HEIGHT = 3600;

    private final MerisPixelProductAreaProvider areaProvider;

    public MerisStrategy() {
        areaProvider = new MerisPixelProductAreaProvider();
    }

    @Override
    public PixelProductArea getArea(String identifier) {
        return areaProvider.getArea(identifier);
    }

    @Override
    public PixelProductArea[] getAllAreas() {
        return areaProvider.getAllAreas();
    }

    @Override
    public String getSensorName() {
        return "MERIS";
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return MerisPixelInputFormat.class;
    }

    @Override
    public int getRasterWidth() {
        return RASTER_WIDTH;
    }

    @Override
    public int getRasterHeight() {
        return RASTER_HEIGHT;
    }

    @Override
    public String getDoyBandName() {
        return "band_1";
    }

    @Override
    public String getClBandName() {
        return "band_2";
    }

    @Override
    public String getTile(boolean mosaicBA, String[] paths) {
        return mosaicBA ? getTileFromBA(paths[0]) : getTileFromLC(paths[1]);
    }

    private static String getTileFromBA(String path) {
        int startIndex = path.indexOf("BA_PIX_MER_") + "BA_PIX_MER_".length();
        return path.substring(startIndex, startIndex + 6);
    }

    private static String getTileFromLC(String path) {
        return path.substring(path.length() - 9, path.length() - 3);
    }

    private static class MerisPixelProductAreaProvider implements PixelProductAreaProvider {

        private enum MerisPixelProductArea {
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

            MerisPixelProductArea(int left, int top, int right, int bottom, int index, String nicename) {
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
            return translate(MerisPixelProductArea.valueOf(identifier));
        }

        private static PixelProductArea translate(MerisPixelProductArea mppa) {
            return new PixelProductArea(mppa.left, mppa.top, mppa.right, mppa.bottom, mppa.index, mppa.nicename);
        }

        @Override
        public PixelProductArea[] getAllAreas() {
            PixelProductArea[] result = new PixelProductArea[MerisPixelProductArea.values().length];
            MerisPixelProductArea[] values = MerisPixelProductArea.values();
            for (int i = 0; i < values.length; i++) {
                MerisPixelProductArea merisPixelProductArea = values[i];
                result[i] = translate(merisPixelProductArea);
            }
            return result;
        }
    }

}
