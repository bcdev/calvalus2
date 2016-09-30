package com.bc.calvalus.processing.fire.format;

import org.apache.hadoop.mapreduce.InputFormat;
import org.esa.snap.core.datamodel.Product;

public interface SensorStrategy {

    PixelProductArea getArea(String identifier);

    PixelProductArea[] getAllAreas();

    Class<? extends InputFormat> getInputFormatClass();

    SensorGeometry getGeometry(boolean mosaicBA, String pathName);

    String getDoyBandName();

    String getClBandName();

    String getTile(boolean mosaicBA, String[] paths);

    Product reproject(Product sourceProduct);

    interface PixelProductAreaProvider {

        PixelProductArea getArea(String identifier);

        PixelProductArea[] getAllAreas();
    }
}
