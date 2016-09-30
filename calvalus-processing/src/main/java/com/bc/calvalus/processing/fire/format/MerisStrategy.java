package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.processing.fire.format.pixel.MerisPixelInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.esa.snap.core.datamodel.Product;

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
    public int getRasterWidth(boolean mosaicBA, String pathName) {
        return RASTER_WIDTH;
    }

    @Override
    public int getRasterHeight(boolean mosaicBA, String pathName) {
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

    @Override
    public int computeTargetHeight(PixelProductArea area) {
        return (area.bottom - area.top) * 360;
    }

    @Override
    public int computeTargetWidth(PixelProductArea area) {
        return (area.right - area.left) * 360;
    }

    @Override
    public int getLeftTargetXForTile(PixelProductArea area, String key) {
        int tileX = Integer.parseInt(key.substring(12));
        if (tileX * 10 > area.left) {
            return (tileX * 10 - area.left) * 360;
        }
        return 0;
    }

    @Override
    public int getTopTargetYForTile(PixelProductArea area, String key) {
        int tileY = Integer.parseInt(key.substring(9, 11));
        if (tileY * 10 > area.top) {
            return (tileY * 10 - area.top) * 360;
        }
        return 0;
    }

    @Override
    public int getLeftSourceXForTile(PixelProductArea area, String key) {
        int tileX = Integer.parseInt(key.substring(12));
        if (tileX * 10 < area.left) {
            return (area.left - tileX * 10) * 360;
        }
        return 0;
    }

    @Override
    public int getMaxSourceXForTile(PixelProductArea area, String key) {
        int tileX = Integer.parseInt(key.substring(12));
        if ((tileX + 1) * 10 > area.right) {
            return (area.right - tileX * 10) * 360 - 1;
        }
        return 3599;
    }

    @Override
    public int getTopSourceYForTile(PixelProductArea area, String key) {
        int tileY = Integer.parseInt(key.substring(9, 11));
        if (tileY * 10 < area.top) {
            return (area.top - tileY * 10) * 360;
        }
        return 0;
    }

    @Override
    public int getMaxSourceYForTile(PixelProductArea area, String key) {
        int tileY = Integer.parseInt(key.substring(9, 11));
        if ((tileY + 1) * 10 > area.bottom) {
            return (area.bottom - tileY * 10) * 360 - 1;
        }
        return 3599;
    }

    @Override
    public double getTargetPixelSize(PixelProductArea area) {
        return 1 / 360.0;
    }

    @Override
    public Product reproject(Product sourceProduct) {
        return sourceProduct;
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
