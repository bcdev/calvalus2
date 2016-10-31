package com.bc.calvalus.processing.fire.format;

import org.esa.snap.binning.AbstractAggregator;
import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.AggregatorDescriptor;
import org.esa.snap.binning.BinContext;
import org.esa.snap.binning.Observation;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.Vector;
import org.esa.snap.binning.WritableVector;
import org.esa.snap.core.gpf.annotations.Parameter;

public class JDAggregator extends AbstractAggregator {

    public static final int WATER = 997;
    public static final int CLOUD = 998;
    public static final int NO_DATA = 999;

    public JDAggregator(String name, String[] spatialFeatureNames, String[] temporalFeatureNames, String[] outputFeatureNames) {
        super(name, spatialFeatureNames, temporalFeatureNames, outputFeatureNames);
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
        vector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Observation observationVector, WritableVector spatialVector) {
        float jd = observationVector.get(0);
        float cl = observationVector.get(1);
        aggregate(jd, cl, ctx, spatialVector);
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
        // nothing to be done, spatialVector already contains correct JD
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
        vector.set(1, 0.0f);
        ctx.put("maxJD", -1.0f);
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        float jd = spatialVector.get(0);
        float cl = spatialVector.get(1);
        aggregate(jd, cl, ctx, temporalVector);
    }

    static void aggregate(float jd, float cl, BinContext ctx, WritableVector targetVector) {
        Float oldMaxJD = ctx.get("maxJD");

        // take max of JD
        // overwrite if JD > old maxJD
        boolean validJD = jd >= 0 && jd < 997;
        boolean newValidMax = oldMaxJD >= 997 || jd > oldMaxJD;
        boolean jdIsSet = oldMaxJD >= 0;

        if (validJD && newValidMax) {
            ctx.put("maxJD", jd);
            targetVector.set(0, jd);
            targetVector.set(1, cl);
        } else if (oldMaxJD == WATER) {
            // don't overwrite water: keep the old value
        } else if (oldMaxJD == CLOUD && jd == WATER) {
            // overwrite cloud with water
            targetVector.set(0, jd);
            targetVector.set(1, cl);
            ctx.put("maxJD", jd);
        } else if (oldMaxJD == NO_DATA && jd != NO_DATA) {
            // overwrite no data with data
            targetVector.set(0, jd);
            targetVector.set(1, cl);
            ctx.put("maxJD", jd);
        } else if (!jdIsSet) {
            targetVector.set(0, jd);
            targetVector.set(1, cl);
            ctx.put("maxJD", jd);
        }
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        // nothing to be done, temporalVector already contains correct JD
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        outputVector.set(0, temporalVector.get(0));
        outputVector.set(1, temporalVector.get(1));
    }

    public static class Config extends AggregatorConfig {

        @Parameter(label = "DOY band name", notEmpty = true, notNull = true, description = "The DOY band used for aggregation.")
        String doyName;
        @Parameter(label = "CL band name", notEmpty = true, notNull = true, description = "The CL band used for aggregation.")
        String clName;

        public Config() {
            this(null, null);
        }

        public Config(String doyName, String clName) {
            super(Descriptor.NAME);
            this.doyName = doyName;
            this.clName = clName;
        }
    }

    public static class Descriptor implements AggregatorDescriptor {

        public static final String NAME = "JD";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Aggregator createAggregator(VariableContext varCtx, AggregatorConfig aggregatorConfig) {
            return new JDAggregator(NAME, new String[]{"JD", "CL"}, new String[]{"JD", "CL"}, getTargetVarNames(aggregatorConfig));
        }

        @Override
        public AggregatorConfig createConfig() {
            return new Config();
        }

        @Override
        public String[] getSourceVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            return new String[]{config.doyName, config.clName};
        }

        @Override
        public String[] getTargetVarNames(AggregatorConfig aggregatorConfig) {
            return new String[]{"JD", "CL"};
        }
    }
}
