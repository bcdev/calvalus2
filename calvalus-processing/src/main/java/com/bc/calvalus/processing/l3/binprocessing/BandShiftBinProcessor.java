package com.bc.calvalus.processing.l3.binprocessing;

import com.bc.ceres.binding.PropertySet;
import org.esa.beam.binning.VariableContext;

/**
 * just an example....
 */
public class BandShiftBinProcessor extends BinProcessor {
    private int numRRS = 6;
    private int numQAA = 3;

    @Override
    public float[] process(float[] featureValues) {
        int index = 0;
        float[] rrs = new float[numRRS];
        float[] qaa = new float[numQAA];
        for (int i = 0; i < rrs.length; i++) {
            rrs[i] = featureValues[index];
            index += 2;
        }
        for (int i = 0; i < qaa.length; i++) {
            qaa[i] = featureValues[index];
            index += 2;
        }
        float[] correctedRss = bandshifting(rrs, qaa);
        return correctedRss;
    }

    private float[] bandshifting(float[] rrs, float[] qaa) {
        return new float[0];  //TODO
    }

    public static class BandShiftBinProcessorDescriptor implements BinProcessorDescriptor {

        public static final String NAME = "BandShift";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public BinProcessor createBinProcessor(VariableContext varCtx, BinProcessorConfig binProcessorConfig) {
            PropertySet currentlyUnusedPropertySet = binProcessorConfig.asPropertySet();
            return new BandShiftBinProcessor();
        }

        @Override
        public BinProcessorConfig createBinProcessorConfig() {
            return new BandShiftBinConfig();
        }
    }

    public static class BandShiftBinConfig extends BinProcessorConfig {

        public BandShiftBinConfig() {
            super(BandShiftBinProcessorDescriptor.NAME);
        }

        @Override
        public String[] getVarNames() {
            return new String[0];
        }
    }
}
