package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.commons.CalvalusLogger;
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

import java.time.Year;

public class JDAggregator extends AbstractAggregator {

    public static final int WATER = 997;
    public static final int CLOUD = 998;
    public static final int NO_DATA = 999;
    private final int minDoy;
    private final int maxDoy;

    public JDAggregator(String name, String[] spatialFeatureNames, String[] temporalFeatureNames, String[] outputFeatureNames, int[] doyBounds) {
        super(name, spatialFeatureNames, temporalFeatureNames, outputFeatureNames);
        this.minDoy = doyBounds[0];
        this.maxDoy = doyBounds[1];
        CalvalusLogger.getLogger().info("minDoy=" + minDoy);
        CalvalusLogger.getLogger().info("maxDoy=" + maxDoy);
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

    void aggregate(float jd, float cl, BinContext ctx, WritableVector targetVector) {
        Float oldMaxJD = ctx.get("maxJD");

        // take max of JD
        // overwrite if JD > old maxJD
        boolean inTimeBounds = jd >= minDoy && jd <= maxDoy;
        boolean validJD = jd >= 0 && jd < 997 && inTimeBounds;
        boolean maybeNewMax = oldMaxJD >= 997 || jd > oldMaxJD;
        boolean jdIsSet = oldMaxJD >= 0;

        if (validJD && maybeNewMax) {
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
        } else if (!jdIsSet && inTimeBounds) {
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
        @Parameter(label = "Year", notEmpty = true, notNull = true, description = "The year in which the aggregation will be done.")
        public int year;
        @Parameter(label = "Month", notEmpty = true, notNull = true, description = "The month in which the aggregation will be done.")
        public int month;

        public Config() {
            this(null, null, -1, -1);
        }

        public Config(String doyName, String clName, int year, int month) {
            super(Descriptor.NAME);
            this.doyName = doyName;
            this.clName = clName;
            this.year = year;
            this.month = month;
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
            Config config = (Config) aggregatorConfig;
            int minDoy = Year.of(config.year).atMonth(config.month).atDay(1).getDayOfYear();
            int maxDoy = Year.of(config.year).atMonth(config.month).atEndOfMonth().getDayOfYear();
            return new JDAggregator(NAME, new String[]{"JD", "CL"}, new String[]{"JD", "CL"}, getTargetVarNames(aggregatorConfig), new int[]{minDoy, maxDoy});
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
