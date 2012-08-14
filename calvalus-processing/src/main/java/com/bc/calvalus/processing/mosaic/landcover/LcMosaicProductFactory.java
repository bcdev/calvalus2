package com.bc.calvalus.processing.mosaic.landcover;

import com.bc.calvalus.processing.mosaic.DefaultMosaicProductFactory;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.ColorPaletteDef;
import org.esa.beam.framework.datamodel.ImageInfo;
import org.esa.beam.framework.datamodel.IndexCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.awt.Color;
import java.awt.Rectangle;

/**
 * The factory for creating the final mosaic product for LC-CCI
 *
 * @author MarcoZ
 */
class LcMosaicProductFactory extends DefaultMosaicProductFactory {

    static final float[] WAVELENGTH = new float[]{
            412.691f, 442.55902f, 489.88202f, 509.81903f, 559.69403f,
            619.601f, 664.57306f, 680.82104f, 708.32904f, 753.37103f,
            761.50806f, 778.40906f, 864.87604f, 884.94403f, 900.00006f};

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

        for (String counter : LCMosaicAlgorithm.COUNTER_NAMES) {
            band = product.addBand(counter + "_count", ProductData.TYPE_INT8);
            band.setNoDataValue(-1);
            band.setNoDataValueUsed(true);
        }
        for (int i = 0; i < AbstractLcMosaicAlgorithm.NUM_SDR_BANDS; i++) {
            int bandIndex = i + 1;
            band = product.addBand("sr_" + bandIndex + "_mean", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
            band.setSpectralBandIndex(bandIndex);
            band.setSpectralWavelength(WAVELENGTH[i]);
        }
        band = product.addBand("ndvi_mean", ProductData.TYPE_FLOAT32);
        band.setNoDataValue(Float.NaN);
        band.setNoDataValueUsed(true);
        for (int i = 0; i < AbstractLcMosaicAlgorithm.NUM_SDR_BANDS; i++) {
            band = product.addBand("sr_" + (i + 1) + "_sigma", ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
        }
        product.setAutoGrouping("mean:sigma:count");

        //TODO
        //product.setStartTime(formatterConfig.getStartTime());
        //product.setEndTime(formatterConfig.getEndTime());
        return product;
    }
}
