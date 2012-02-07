package com.bc.calvalus.processing.mosaic;

import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.datamodel.ProductNodeFilter;
import org.esa.beam.framework.gpf.annotations.SourceProduct;
import org.esa.beam.framework.gpf.pointop.PixelOperator;
import org.esa.beam.framework.gpf.pointop.ProductConfigurer;
import org.esa.beam.framework.gpf.pointop.Sample;
import org.esa.beam.framework.gpf.pointop.SampleConfigurer;
import org.esa.beam.framework.gpf.pointop.WritableSample;

import java.io.IOException;


class UclCloudOperator extends PixelOperator {

    @SourceProduct
    Product sourceProduct;

    private UclCloudDetection uclCloudDetection;

    @Override
    protected void configureTargetProduct(ProductConfigurer pc) {
        super.configureTargetProduct(pc);

        pc.copyBands(new ProductNodeFilter<Band>() {
            @Override
            public boolean accept(Band productNode) {
                String name = productNode.getName();
                return name.startsWith("sdr_") || name.equals("status");
            }
        });

        pc.addBand("hue", ProductData.TYPE_FLOAT32, Float.NaN);
        pc.addBand("sat", ProductData.TYPE_FLOAT32, Float.NaN);
        pc.addBand("val", ProductData.TYPE_FLOAT32, Float.NaN);
        pc.addBand("cloudCoeff", ProductData.TYPE_FLOAT32, Float.NaN);
        pc.addBand("landCoeff", ProductData.TYPE_FLOAT32, Float.NaN);
        pc.addBand("probability" +
                           "", ProductData.TYPE_FLOAT32, Float.NaN);

        try {
            uclCloudDetection = UclCloudDetection.create();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void configureSourceSamples(SampleConfigurer sampleConfigurer) {
        sampleConfigurer.defineSample(0, "sdr_7");
        sampleConfigurer.defineSample(1, "sdr_14");
        sampleConfigurer.defineSample(2, "sdr_3");
    }

    @Override
    public void configureTargetSamples(SampleConfigurer sampleConfigurer) {
        sampleConfigurer.defineSample(0, "hue");
        sampleConfigurer.defineSample(1, "sat");
        sampleConfigurer.defineSample(2, "val");
        sampleConfigurer.defineSample(3, "cloudCoeff");
        sampleConfigurer.defineSample(4, "landCoeff");
        sampleConfigurer.defineSample(5, "probability");
    }

    @Override
    protected void computePixel(int x, int y, Sample[] sourceSamples, WritableSample[] targetSamples) {
        float sdrRed = sourceSamples[0].getFloat();
        if (Float.isNaN(sdrRed)) {
            for (WritableSample targetSample : targetSamples) {
                targetSample.set(Float.NaN);
            }
            return;
        }
        float sdrGreen = sourceSamples[1].getFloat();
        float sdrBlue = sourceSamples[2].getFloat();

        float[] hsv = UclCloudDetection.rgb2hsv(sdrRed, sdrGreen, sdrBlue);
        targetSamples[0].set(hsv[0]);
        targetSamples[1].set(hsv[1]);
        targetSamples[2].set(hsv[2]);

        float cloudCoefficient = uclCloudDetection.cloudScatterData.getCoefficient(hsv);
        targetSamples[3].set(cloudCoefficient);
        float landCoefficient = uclCloudDetection.landScatterData.getCoefficient(hsv);
        targetSamples[4].set(landCoefficient);
        targetSamples[5].set(UclCloudDetection.computeProbability(landCoefficient, cloudCoefficient));
    }

    public static void main(String[] args) throws IOException {
        UclCloudOperator operator = new UclCloudOperator();
        operator.setSourceProduct(ProductIO.readProduct(args[0]));
        ProductIO.writeProduct(operator.getTargetProduct(), "/home/marco/uclcloud.nc", "NetCDF-CF");
    }
}
