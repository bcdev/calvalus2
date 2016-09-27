package com.bc.calvalus.processing.fire.format;

import org.apache.hadoop.mapreduce.InputFormat;

public interface SensorStrategy {

    PixelProductArea getArea(String identifier);

    PixelProductArea[] getAllAreas();

    String getSensorName();

    Class<? extends InputFormat> getInputFormatClass();

    int getRasterWidth();

    int getRasterHeight();

    String getDoyBandName();

    String getClBandName();

    String getTile(boolean mosaicBA, String[] paths);

    interface PixelProductAreaProvider {

        PixelProductArea getArea(String identifier);

        PixelProductArea[] getAllAreas();
    }
}
