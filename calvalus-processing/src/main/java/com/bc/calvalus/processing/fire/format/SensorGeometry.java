package com.bc.calvalus.processing.fire.format;

public interface SensorGeometry {

    int getRasterWidth();

    int getRasterHeight();

    int computeTargetWidth(PixelProductArea area);

    int computeTargetHeight(PixelProductArea area);

    int getLeftTargetXForTile(PixelProductArea area, String key);

    int getTopTargetYForTile(PixelProductArea area, String key);

    int getLeftSourceXForTile(PixelProductArea area, String key);

    int getMaxSourceXForTile(PixelProductArea area, String key);

    int getTopSourceYForTile(PixelProductArea area, String key);

    int getMaxSourceYForTile(PixelProductArea area, String key);

    double getTargetPixelSize(PixelProductArea area);
}
