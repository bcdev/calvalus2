package com.bc.calvalus.processing.fire.format.pixel.olci;

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

public class OlciJDAggregator extends AbstractAggregator {

    public static final int UNBURNABLE = -2;
    public static final int NOT_OBSERVED = -1;
    private final int minDoy;
    private final int maxDoy;

    public OlciJDAggregator(String name, String[] spatialFeatureNames, String[] temporalFeatureNames, String[] outputFeatureNames, int[] doyBounds) {
        super(name, spatialFeatureNames, temporalFeatureNames, outputFeatureNames);
        this.minDoy = doyBounds[0];
        this.maxDoy = doyBounds[1];
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, NOT_OBSERVED);
        vector.set(1, 0.0f);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Observation observationVector, WritableVector spatialVector) {
        float jd = observationVector.get(0);
        float cl = observationVector.get(1);
        aggregate(jd, cl, spatialVector);
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
        // nothing to be done, spatialVector already contains correct JD
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        float jd = spatialVector.get(0);
        float cl = spatialVector.get(1);
        temporalVector.set(0, jd);
        temporalVector.set(1, cl);
    }

    private void aggregate(float jd, float cl, WritableVector targetVector) {
        float previousJDValue = targetVector.get(0);

        // the previous JD is valid if it is not negative and within the time bounds
        boolean validJdSet = previousJDValue >= 0 && previousJDValue >= minDoy && previousJDValue <= maxDoy;
        boolean inTimeBounds = jd >= minDoy && jd <= maxDoy;
        // the new JD is preferred if it is earlier within the time bounds (or "more valid")
        boolean preferToPreviousValue = (!validJdSet || jd < previousJDValue && jd > 0) && jd >= 0 && (inTimeBounds || jd == 0);

        if (preferToPreviousValue) {
            targetVector.set(0, jd);
            targetVector.set(1, cl);
            validJdSet = true;
        } else if (jd == UNBURNABLE && previousJDValue == NOT_OBSERVED) {
            // unburnable beats only not observed
            targetVector.set(0, UNBURNABLE);
            targetVector.set(1, 0);
        }
        //if (!validJdSet && jd == 0 && cl == 0) {
        if (jd >= 0 && cl == 0) {
            targetVector.set(1, 1);
        }
        // otherwise keep original values
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        // nothing to be done, temporalVector already contains correct JD
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        outputVector.set(0, (int) temporalVector.get(0));
        outputVector.set(1, (int) temporalVector.get(1));
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

        public static final String NAME = "OLCI-JD";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Aggregator createAggregator(VariableContext varCtx, AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            int minDoy = Year.of(config.year).atMonth(config.month).atDay(1).getDayOfYear();
            int maxDoy = Year.of(config.year).atMonth(config.month).atEndOfMonth().getDayOfYear();
            return new OlciJDAggregator(NAME, new String[]{config.doyName, config.clName}, new String[]{"JD", "CL"}, getTargetVarNames(aggregatorConfig), new int[]{minDoy, maxDoy});
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
