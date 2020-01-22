package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.pointop.PixelOperator;
import org.esa.snap.core.gpf.pointop.Sample;
import org.esa.snap.core.gpf.pointop.SourceSampleConfigurer;
import org.esa.snap.core.gpf.pointop.TargetSampleConfigurer;
import org.esa.snap.core.gpf.pointop.WritableSample;

public class SlstrRadToReflOp extends PixelOperator {

    double[] solarIrradiances;

    @Override
    protected void computePixel(int i, int i1, Sample[] samples, WritableSample[] writableSamples) {
        double[] rads = new double[12];
        for (int j = 0; j < rads.length; j++) {
            rads[j] = samples[j].getDouble();
        }

        double szaNadir = samples[12].getDouble();
        double szaOblique = samples[13].getDouble();

        for (int j = 0; j < 6; j++) {
            writableSamples[j].set((rads[j] * Math.PI) / (solarIrradiances[j] * Math.cos(szaNadir)));
        }

        for (int j = 6; j < 12; j++) {
            writableSamples[j].set((rads[j] * Math.PI) / (solarIrradiances[j] * Math.cos(szaOblique)));
        }

    }

    @Override
    protected Product createTargetProduct() throws OperatorException {
        Product sourceProduct = getSourceProduct();
        solarIrradiances = new double[12];

        for (int i = 0; i < 6; i++) {
            solarIrradiances[i] = sourceProduct.getMetadataRoot().getElement("S" + i).getElement("S" + i + "_solar_irradiance_an").getAttributeDouble("value.1");;
        }

        for (int i = 6; i < 9; i++) {
            solarIrradiances[i] = sourceProduct.getMetadataRoot().getElement("S" + (i - 6)).getElement("S" + (i - 6) + "_solar_irradiance_ao").getAttributeDouble("value.1");;
        }

        for (int i = 9; i < 12; i++) {
            solarIrradiances[i] = sourceProduct.getMetadataRoot().getElement("S" + (i - 6)).getElement("S" + (i - 6) + "_solar_irradiance_co").getAttributeDouble("value.1");;
        }

        return super.createTargetProduct();
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
        targetSampleConfigurer.defineSample(0, "S1_radiance_an");
        targetSampleConfigurer.defineSample(1, "S2_radiance_an");
        targetSampleConfigurer.defineSample(2, "S3_radiance_an");
        targetSampleConfigurer.defineSample(3, "S4_radiance_cn");
        targetSampleConfigurer.defineSample(4, "S5_radiance_cn");
        targetSampleConfigurer.defineSample(5, "S6_radiance_cn");

        targetSampleConfigurer.defineSample(6, "S1_radiance_ao");
        targetSampleConfigurer.defineSample(7, "S2_radiance_ao");
        targetSampleConfigurer.defineSample(8, "S3_radiance_ao");
        targetSampleConfigurer.defineSample(9, "S4_radiance_co");
        targetSampleConfigurer.defineSample(10, "S5_radiance_co");
        targetSampleConfigurer.defineSample(11, "S6_radiance_co");
    }
}
