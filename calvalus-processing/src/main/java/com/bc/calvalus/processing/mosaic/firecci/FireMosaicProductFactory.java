/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.mosaic.firecci;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.mosaic.DefaultMosaicProductFactory;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ColorPaletteDef;
import org.esa.snap.core.datamodel.ImageInfo;
import org.esa.snap.core.datamodel.IndexCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.awt.Color;
import java.awt.Rectangle;
import com.bc.calvalus.processing.mosaic.landcover.LcL3SensorConfig;
import org.apache.hadoop.conf.Configuration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The factory for creating the final mosaic product for Fire-CCI
 *
 * @author MarcoZ
 * @author thomas
 */
class FireMosaicProductFactory extends DefaultMosaicProductFactory {

    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    static final float[] MERIS_WAVELENGTH = new float[]{
            412.691f,   // 1
            442.55902f, // 2
            489.88202f, // 3
            509.81903f, // 4
            559.69403f, // 5
            619.601f,   // 6
            664.57306f, // 7
            680.82104f, // 8
            708.32904f, // 9
            753.37103f, // 10
            //761.50806f, // 11
            778.40906f, // 12
            864.87604f, // 13
            884.94403f, // 14
            //900.00006f  // 15
    };
    private Configuration configuration;
    private int tileX;
    private int tileY;

    public FireMosaicProductFactory(String[] outputFeatures) {
        super(outputFeatures);
    }

    @Override
    public Product createProduct(Configuration configuration, int tileX, int tileY, Rectangle rect) {
        this.configuration = configuration;
        this.tileX = tileX;
        this.tileY = tileY;
        return super.createProduct(configuration, tileX, tileY, rect);
    }

    @Override
    public Product createProduct(String productName, Rectangle rect) {
        final Product product = new Product(productName, "CALVALUS-Mosaic", rect.width, rect.height);

        Band band = product.addBand("status", ProductData.TYPE_INT8);
        band.setNoDataValue(0);
        band.setNoDataValueUsed(true);

        final IndexCoding indexCoding = new IndexCoding("status");
        ColorPaletteDef.Point[] points = new ColorPaletteDef.Point[5];
        indexCoding.addIndex("land", 1, "");
        points[0] = new ColorPaletteDef.Point(1, Color.GREEN, "land");
        indexCoding.addIndex("water", 2, "");
        points[1] = new ColorPaletteDef.Point(2, Color.BLUE, "water");
        indexCoding.addIndex("snow", 3, "");
        points[2] = new ColorPaletteDef.Point(3, Color.YELLOW, "snow");
        indexCoding.addIndex("cloud", 4, "");
        points[3] = new ColorPaletteDef.Point(4, Color.WHITE, "cloud");
        indexCoding.addIndex("cloud_shadow", 5, "");
        points[4] = new ColorPaletteDef.Point(5, Color.GRAY, "cloud_shadow");
        product.getIndexCodingGroup().add(indexCoding);
        band.setSampleCoding(indexCoding);
        band.setImageInfo(new ImageInfo(new ColorPaletteDef(points, points.length)));

        String[] outputFeatures = getOutputFeatures();
        int sdrBandIndex = 0;
        for (int i = 1; i < outputFeatures.length; i++) {
            String outputFeature = outputFeatures[i];
            band = product.addBand(outputFeature, ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
            if (outputFeature.matches("sdr_(\\d+)")) {
                band.setSpectralBandIndex(sdrBandIndex+1);
                band.setSpectralWavelength(MERIS_WAVELENGTH[sdrBandIndex]);
                sdrBandIndex++;
            }
        }

        LcL3SensorConfig sensorConfig = LcL3SensorConfig.create(configuration.get("calvalus.lc.resolution"));
        product.getMetadataRoot().setAttributeString("sensor", sensorConfig.getSensorName());
        product.getMetadataRoot().setAttributeString("platform", sensorConfig.getPlatformName());
        product.getMetadataRoot().setAttributeString("spatialResolution", sensorConfig.getGroundResolution());
        product.getMetadataRoot().setAttributeString("temporalResolution", "P1D");  // TODO
        product.getMetadataRoot().setAttributeString("version", configuration.get(JobConfigNames.CALVALUS_LC_VERSION, "2.0"));
        product.getMetadataRoot().setAttributeInt("tileY", tileY);
        product.getMetadataRoot().setAttributeInt("tileX", tileX);

        String minDateParameter = configuration.get("calvalus.minDate");
        String maxDateParameter = configuration.get("calvalus.maxDate");
        final Date minDate;
        final Date maxDate;
        try {
            minDate = DATE_FORMAT.parse(minDateParameter);
            maxDate = DATE_FORMAT.parse(maxDateParameter);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        product.setStartTime(ProductData.UTC.create(minDate, 0));
        product.setEndTime(ProductData.UTC.create(maxDate, 0));

        return product;
    }
}
