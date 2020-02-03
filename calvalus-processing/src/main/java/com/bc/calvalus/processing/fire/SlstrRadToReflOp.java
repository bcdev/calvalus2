package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.OperatorSpi;
import org.esa.snap.core.gpf.annotations.OperatorMetadata;
import org.esa.snap.core.gpf.annotations.SourceProduct;
import org.esa.snap.core.gpf.pointop.PixelOperator;
import org.esa.snap.core.gpf.pointop.Sample;
import org.esa.snap.core.gpf.pointop.SourceSampleConfigurer;
import org.esa.snap.core.gpf.pointop.TargetSampleConfigurer;
import org.esa.snap.core.gpf.pointop.WritableSample;

import java.util.HashMap;

@OperatorMetadata(alias = "SlstrRadToRefl",
        category = "Raster/Geometric",
        version = "0.1",
        authors = "Thomas Storm",
        copyright = "(c) 2020 by Brockmann Consult",
        description = "Converts SLSTR radiances to reflectances")
public class SlstrRadToReflOp extends PixelOperator {

    @SourceProduct
    Product sourceProduct;

    double[] solarIrradiances;

    @Override
    protected void prepareInputs() throws OperatorException {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("bandNames", new String[]{"S1_radiance_an", "S2_radiance_an", "S3_radiance_an", "S4_radiance_cn", "S5_radiance_cn", "S6_radiance_cn", "S1_radiance_ao", "S2_radiance_ao", "S3_radiance_ao", "S4_radiance_co", "S5_radiance_co", "S6_radiance_co"});
        parameters.put("tiePointGridNames", new String[]{"solar_zenith_tn", "solar_zenith_to"});
        parameters.put("copyMetadata", true);
        sourceProduct = GPF.createProduct("Subset", parameters, sourceProduct);
    }

    @Override
    protected void computePixel(int x, int y, Sample[] samples, WritableSample[] writableSamples) {
        double[] rads = new double[12];
        for (int j = 0; j < rads.length; j++) {
            rads[j] = samples[j].getDouble();
        }

        double szaNadir = samples[12].getDouble();
        double szaOblique = samples[13].getDouble();

        for (int j = 0; j < 6; j++) {
            writableSamples[j].set((rads[j] * Math.PI) / (solarIrradiances[j] * Math.cos(Math.toRadians(szaNadir))));
        }

        for (int j = 6; j < 12; j++) {
            writableSamples[j].set((rads[j] * Math.PI) / (solarIrradiances[j] * Math.cos(Math.toRadians(szaOblique))));
        }

    }

    @Override
    protected Product createTargetProduct() throws OperatorException {
        solarIrradiances = new double[12];

        for (int i = 1; i < 7; i++) {
            solarIrradiances[i - 1] = sourceProduct.getMetadataRoot().getElement("S" + i).getElement("S" + i + "_solar_irradiance_an").getAttributeDouble("value");
        }

        for (int i = 7; i < 10; i++) {
            solarIrradiances[i - 1] = sourceProduct.getMetadataRoot().getElement("S" + (i - 6)).getElement("S" + (i - 6) + "_solar_irradiance_ao").getAttributeDouble("value");
        }

        for (int i = 10; i < 13; i++) {
            solarIrradiances[i - 1] = sourceProduct.getMetadataRoot().getElement("S" + (i - 6)).getElement("S" + (i - 6) + "_solar_irradiance_co").getAttributeDouble("value");
        }

        Product targetProduct = super.createTargetProduct();
        targetProduct.addBand("S1_reflectance_an", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S2_reflectance_an", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S3_reflectance_an", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S4_reflectance_cn", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S5_reflectance_cn", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S6_reflectance_cn", ProductData.TYPE_FLOAT32);

        targetProduct.addBand("S1_reflectance_ao", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S2_reflectance_ao", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S3_reflectance_ao", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S4_reflectance_co", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S5_reflectance_co", ProductData.TYPE_FLOAT32);
        targetProduct.addBand("S6_reflectance_co", ProductData.TYPE_FLOAT32);

        return targetProduct;
    }

    @Override
    protected void configureSourceSamples(SourceSampleConfigurer sourceSampleConfigurer) throws OperatorException {
        sourceSampleConfigurer.defineSample(0, "S1_radiance_an");
        sourceSampleConfigurer.defineSample(1, "S2_radiance_an");
        sourceSampleConfigurer.defineSample(2, "S3_radiance_an");
        sourceSampleConfigurer.defineSample(3, "S4_radiance_cn");
        sourceSampleConfigurer.defineSample(4, "S5_radiance_cn");
        sourceSampleConfigurer.defineSample(5, "S6_radiance_cn");

        sourceSampleConfigurer.defineSample(6, "S1_radiance_ao");
        sourceSampleConfigurer.defineSample(7, "S2_radiance_ao");
        sourceSampleConfigurer.defineSample(8, "S3_radiance_ao");
        sourceSampleConfigurer.defineSample(9, "S4_radiance_co");
        sourceSampleConfigurer.defineSample(10, "S5_radiance_co");
        sourceSampleConfigurer.defineSample(11, "S6_radiance_co");

        sourceSampleConfigurer.defineSample(12, "solar_zenith_tn");
        sourceSampleConfigurer.defineSample(13, "solar_zenith_to");
    }

    @Override
    protected void configureTargetSamples(TargetSampleConfigurer targetSampleConfigurer) throws OperatorException {
        targetSampleConfigurer.defineSample(0, "S1_reflectance_an");
        targetSampleConfigurer.defineSample(1, "S2_reflectance_an");
        targetSampleConfigurer.defineSample(2, "S3_reflectance_an");
        targetSampleConfigurer.defineSample(3, "S4_reflectance_cn");
        targetSampleConfigurer.defineSample(4, "S5_reflectance_cn");
        targetSampleConfigurer.defineSample(5, "S6_reflectance_cn");

        targetSampleConfigurer.defineSample(6, "S1_reflectance_ao");
        targetSampleConfigurer.defineSample(7, "S2_reflectance_ao");
        targetSampleConfigurer.defineSample(8, "S3_reflectance_ao");
        targetSampleConfigurer.defineSample(9, "S4_reflectance_co");
        targetSampleConfigurer.defineSample(10, "S5_reflectance_co");
        targetSampleConfigurer.defineSample(11, "S6_reflectance_co");
    }

    public static class Spi extends OperatorSpi {

        public Spi() {
            super(SlstrRadToReflOp.class);
        }
    }
}
