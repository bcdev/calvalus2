package com.bc.calvalus.processing.fire.format.pixel.s2;

import org.esa.snap.binning.AbstractAggregator;
import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.AggregatorDescriptor;
import org.esa.snap.binning.BinContext;
import org.esa.snap.binning.Observation;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.Vector;
import org.esa.snap.binning.WritableVector;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.annotations.Parameter;

import java.time.Year;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;


/**
 * Aggregator for BA monthly pixel products with JD and CL. The aggregator
 * determines the earliest BA contribution within a month, but excludes a BA
 * if BA has been detected in the 39 days before. The aggregator also excludes
 * BA if a WATER contribution is found, and it distinguishes CLEAR_LAND, CLOUD
 * and NO_DATA as lower priority contributions if there is no BA.
 *
 * The temporal aggregator gets observations incrementally (not necessarily in
 * temporal sequence) and maintains state of the highest priority it has seen
 * in a triple of <mjd, jd, cl> .
 *  o jd represents the pixel type, one of WATER, BA, CLEAR_LAND, CLOUD, NO_DATA
 *    in this priority sequence. BA is a value between 1..366 . Default is NO_DATA.
 *  o mjd is the date of a BA, either within the month (then jd is BA), or before it
 *    (the JD is any other, whatever has been seen in month). Default is NO_TIME.
 *  o cl is set only in case of BA. Default is 0.
 * Spatial aggregation plainly forwards the tuple and does not follow this encoding.
 * Examples of temporal intermediate states:
 *    <2345.54, 33, 0.9>  a BA at the 2nd of February, if February is the month to be processed
 *    <2339.54, 999, 0>   a BA at the 27th of January, if February is processed, but no February pixel has been processed so far
 *    <2339.54, 0, 0>     a BA at the 27th of January, if February is processed, and we have seen CLEAR_LAND in February
 *    <-1, 998, 0>        no BA so far, CLOUD has been seen in February
 *    <-1, 0, 0>          no BA so far, CLEAR_LAND has been seen in February
 */

public class S2JDAggregator extends AbstractAggregator {

    public static final float CLEAR_LAND = 0;
    public static final float WATER = 997;
    public static final float CLOUD = 998;
    public static final float NO_DATA = 999;
    public static final int PRE_CLOUD = -100;
    public static final int PRE_EMPTY = -101;
    private static final float NO_TIME = -1.0f;

    private final int minDoy;
    private final int maxDoy;
    private final float minMjd;
    private final float maxMjd;

    public S2JDAggregator(String name, String[] spatialFeatureNames, String[] temporalFeatureNames, String[] outputFeatureNames, int[] doyBounds, float minMjd, float maxMjd) {
        super(name, spatialFeatureNames, temporalFeatureNames, outputFeatureNames);
        this.minDoy = doyBounds[0];
        this.maxDoy = doyBounds[1];
        this.minMjd = minMjd;
        this.maxMjd = maxMjd;
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        // we start with the lowest priority no data and without BA time marker
        vector.set(0, NO_TIME);
        vector.set(1, NO_DATA);
        vector.set(2, 0.0f);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Observation observationVector, WritableVector spatialVector) {
        double mjd = observationVector.getMJD();
        float jd = observationVector.get(0);
        float cl = observationVector.get(1);
        if (mjd == 0.0) {
            throw new IllegalArgumentException("input product lacks time information");
        }
        // pass through the contribution
        setAggregate((float) mjd, jd, cl, spatialVector);
        //System.out.println("agg " + ctx.getIndex() + " contrib -> " + spatialVector.get(0) + " " + spatialVector.get(1) + " " + spatialVector.get(2));
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
        // nothing to be done
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        // we start with the lowest priority no data and without BA time marker
        vector.set(0, NO_TIME);
        vector.set(1, NO_DATA);
        vector.set(2, 0.0f);
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        float mjd = spatialVector.get(0);
        float jd = spatialVector.get(1);
        float cl = spatialVector.get(2);
        aggregate(mjd, jd, cl, temporalVector);
        //System.out.println("agg " + ctx.getIndex() + " contrib " + mjd + " " + jd + " " + cl + " -> " + temporalVector.get(0) + " " + temporalVector.get(1) + " " + temporalVector.get(2));
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        // nothing to be done, temporalVector already contains correct JD
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        outputVector.set(0, temporalVector.get(1));
        outputVector.set(1, temporalVector.get(2));
    }

    void aggregate(float obs_mjd, float obs_jd, float obs_cl, WritableVector state) {
        final float agg_mjd = state.get(0);
        final float agg_jd = state.get(1);
        final float agg_cl = state.get(2);
        // are we within the month?
        if (isInMonth(obs_mjd)) {
            // prefer water
            if (agg_jd == WATER) {
                // pass through
            } else if (obs_jd == WATER) {
                setAggregate(WATER, 0.0f, state);
            }
            // substitute BA with CLEAR if there is BA before the month
            else if (isBurned(obs_jd) && isBeforeMonth(agg_mjd)) {
                setAggregate(CLEAR_LAND, 0.0f, state);
            }
            // prefer earlier BA within months (agg BA is in the month, checked by the condition above)
            // for BA at the same date prefer higher CL (for adjacent granules with overlap)
            else if (isBurned(obs_jd) && (!isBurned(agg_jd) || (obs_mjd < agg_mjd || (obs_mjd == agg_mjd && obs_cl > agg_cl)))) {
                setAggregate(obs_mjd, obs_jd, obs_cl, state);
            }
            // seen BA has next priority
            else if (isBurned(agg_jd)) {
                // pass through
            }
            // clear land is next priority
            else if (agg_jd == CLEAR_LAND) {
                // pass through
            } else if (obs_jd == CLEAR_LAND) {
                setAggregate(CLEAR_LAND, 0.0f, state);
            }
            // cloud is next priority
            else if (agg_jd == CLOUD) {
                // pass through
            } else if (obs_jd == CLOUD || obs_jd == PRE_CLOUD) {
                setAggregate(CLOUD, 0.0f, state);
            }
            // no data is the initial value already, no change required
            // pass through
        } else if (isBeforeMonth(obs_mjd)) {
            // before the month we only consider BA
            if (isBurned(obs_jd)) {
                // replace BA with CLEAR if there was BA within the month
                if (isBurned(agg_jd)) {
                    setAggregate(CLEAR_LAND, 0.0f, state);
                }
                // set marker that we have seen BA before the month
                setAggregate(obs_mjd, state);
            }
        }
    }

    private static void setAggregate(float mjd, WritableVector state) {
        state.set(0, mjd);
    }

    private static void setAggregate(float jd, float cl, WritableVector state) {
        state.set(1, jd);
        state.set(2, cl);
    }

    private static void setAggregate(float mjd, float jd, float cl, WritableVector state) {
        state.set(0, mjd);
        state.set(1, jd);
        state.set(2, cl);
    }

    private boolean isInMonth(float obs_mjd) {
        return obs_mjd != NO_TIME && obs_mjd >= minMjd && obs_mjd < maxMjd;
    }

    private boolean isBeforeMonth(float obs_mjd) {
        return obs_mjd != NO_TIME && obs_mjd < minMjd;
    }

    private static boolean isBurned(float jd) {
        return jd > 0 && jd < 997;
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
            final Config config = (Config) aggregatorConfig;
            final int minDoy = Year.of(config.year).atMonth(config.month).atDay(1).getDayOfYear();
            final int maxDoy = Year.of(config.year).atMonth(config.month).atEndOfMonth().getDayOfYear();
            final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            calendar.clear();
            calendar.set(config.year, config.month-1, 1, 0, 0, 0);
            final float minMjd = (float) ProductData.UTC.create(calendar.getTime(), 0).getMJD();
            calendar.add(Calendar.MONTH, 1);
            final float maxMjd = (float) ProductData.UTC.create(calendar.getTime(), 0).getMJD();
            return new S2JDAggregator(NAME,
                                      new String[]{"MJD", "JD", "CL"},
                                      new String[]{"MJD", "JD", "CL"},
                                      getTargetVarNames(aggregatorConfig),
                                      new int[]{minDoy, maxDoy},
                                      minMjd, maxMjd);
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
