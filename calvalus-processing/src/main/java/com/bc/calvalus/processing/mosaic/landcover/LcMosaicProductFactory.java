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
import java.util.ArrayList;
import java.util.List;

/**
 * The factory for creating the final mosaic product for LC-CCI
 *
 * @author MarcoZ
 */
class LcMosaicProductFactory extends DefaultMosaicProductFactory {

    static final float[] MERIS_WAVELENGTH = new float[]{
            412.691f, 442.55902f, 489.88202f, 509.81903f, 559.69403f,
            619.601f, 664.57306f, 680.82104f, 708.32904f, 753.37103f,
            761.50806f, 778.40906f, 864.87604f, 884.94403f, 900.00006f};
    static final float[] SPOT_WAVELENGTH = new float[]{450f, 645f, 835f, 1665f};

    public LcMosaicProductFactory(String[] outputFeatures) {
        super(outputFeatures);
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
        List<String> countBandNames = new ArrayList<String>(outputFeatures.length);
        List<String> srMeanBandNames = new ArrayList<String>(outputFeatures.length);
        List<String> srSigmaBandNames = new ArrayList<String>(outputFeatures.length);
        for (String outputFeature : outputFeatures) {
            if (outputFeature.endsWith("_count")) {
                countBandNames.add(outputFeature);
            } else if (outputFeature.startsWith("sr_")) {
                if (outputFeature.endsWith("_mean")) {
                    srMeanBandNames.add(outputFeature);
                } else if (outputFeature.endsWith("_sigma")) {
                    srSigmaBandNames.add(outputFeature);
                }
            }
        }

        for (String counter : countBandNames) {
            band = product.addBand(counter, ProductData.TYPE_INT8);
            band.setNoDataValue(-1);
            band.setNoDataValueUsed(true);
        }
        float[] wavelength = outputFeatures.length > 30 ? MERIS_WAVELENGTH : SPOT_WAVELENGTH;
        for (int i = 0; i < srMeanBandNames.size(); i++) {
            int bandIndex = i + 1;
            band = product.addBand(srMeanBandNames.get(i), ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
            band.setSpectralBandIndex(bandIndex);
            band.setSpectralWavelength(wavelength[i]);
        }
        band = product.addBand("ndvi_mean", ProductData.TYPE_FLOAT32);
        band.setNoDataValue(Float.NaN);
        band.setNoDataValueUsed(true);
        for (String srSigmaBandName : srSigmaBandNames) {
            band = product.addBand(srSigmaBandName, ProductData.TYPE_FLOAT32);
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
