package com.bc.calvalus.processing.fire.format;

import org.apache.hadoop.mapreduce.InputFormat;

public interface SensorStrategy {

    PixelProductArea getArea(String identifier);

    PixelProductArea[] getAllAreas();

    String getSensorName();

    Class<? extends InputFormat> getInputFormatClass();

    interface PixelProductAreaProvider {

        PixelProductArea getArea(String identifier);

        PixelProductArea[] getAllAreas();
    }
}
