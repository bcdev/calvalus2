package com.bc.calvalus.processing.beam;

import org.esa.snap.binning.BinContext;
import org.esa.snap.binning.Observation;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.support.ObservationImpl;
import org.esa.snap.binning.support.VectorImpl;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import static org.junit.Assert.*;
import static java.lang.Float.*;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class AggregatorOnPercentileSetTest {
    private BinContext ctx;
    private AggregatorOnPercentileSet agg;

    @Before
    public void setUp() {
        ctx = createCtx();
        agg = new AggregatorOnPercentileSet(new MyVariableContext("a", "b", "c"), "c", "Out", 60,"a", "b");
    }

    @Test
    public void testRequiresGrowableSpatialData() {
        assertFalse(agg.requiresGrowableSpatialData());
    }

    @Test
    public void testMetadata() {
        assertEquals("ON_PERCENTILE_SET", agg.getName());

        assertEquals(4, agg.getSpatialFeatureNames().length);
        assertEquals("Out", agg.getSpatialFeatureNames()[0]);
        assertEquals("c_mjd", agg.getSpatialFeatureNames()[1]);
        assertEquals("a", agg.getSpatialFeatureNames()[2]);
        assertEquals("b", agg.getSpatialFeatureNames()[3]);

        assertEquals(4, agg.getTemporalFeatureNames().length);
        assertEquals("Out", agg.getTemporalFeatureNames()[0]);
        assertEquals("c_mjd", agg.getTemporalFeatureNames()[1]);
        assertEquals("a", agg.getTemporalFeatureNames()[2]);
        assertEquals("b", agg.getTemporalFeatureNames()[3]);

        assertEquals(4, agg.getOutputFeatureNames().length);
        assertEquals("Out", agg.getOutputFeatureNames()[0]);
        assertEquals("c_mjd", agg.getOutputFeatureNames()[1]);
        assertEquals("a", agg.getOutputFeatureNames()[2]);
        assertEquals("b", agg.getOutputFeatureNames()[3]);

    }

    @Test
    public void testAggregatorOnMaxSet() {
        VectorImpl svec = vec(NaN, NaN, NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN, NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(NaN, svec.get(1), 0.0f);
        assertEquals(0.0f, svec.get(2), 0.0f);
        assertEquals(0.0f, svec.get(3), 0.0f);

        agg.aggregateSpatial(ctx, obs(4, 7.3f, 0.5f, 1.1f), svec);
        agg.aggregateSpatial(ctx, obs(5, 0.1f, 2.5f, 1.5f), svec);
        agg.aggregateSpatial(ctx, obs(6, 5.5f, 4.9f, 1.4f), svec);
        assertEquals(4.0f, svec.get(0), 1e-5f);
        assertEquals(6f, svec.get(1), 1e-5f);
        assertEquals(12.9f, svec.get(2), 1e-5f);
        assertEquals(7.9f, svec.get(3), 1e-5f);

        agg.completeSpatial(ctx, 3, svec);
        assertEquals(1.333333333f, svec.get(0), 1e-5f);
        assertEquals(6f, svec.get(1), 1e-5f);
        assertEquals(4.3f, svec.get(2), 1e-5f);
        assertEquals(2.633333333f, svec.get(3), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(NaN, tvec.get(1), 0.0f);
        assertEquals(NaN, tvec.get(2), 0.0f);
        assertEquals(NaN, tvec.get(3), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.3f, 4, 0.2f, 9.7f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(1.1f, 5, 0.1f, 0.3f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(4.7f, 6, 0.6f, 7.1f), 3, tvec);
        agg.completeTemporal(ctx, 3, tvec);
        assertEquals(1.1f, tvec.get(0), 1e-5f);
        assertEquals(5, tvec.get(1), 1e-5f);
        assertEquals(0.1f, tvec.get(2), 1e-5f);
        assertEquals(0.3f, tvec.get(3), 1e-5f);

        agg.computeOutput(tvec, out);
        assertEquals(1.1f, out.get(0), 1e-5f);
        assertEquals(5f, out.get(1), 1e-5f);
        assertEquals(0.1f, out.get(2), 1e-5f);
        assertEquals(0.3f, out.get(3), 1e-5f);
    }

    @Test
    public void testAggregatorOnPercentileSet_AllNaN() {
        VectorImpl svec = vec(NaN, NaN, NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN, NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(NaN, svec.get(1), 0.0f);
        assertEquals(0.0f, svec.get(2), 0.0f);
        assertEquals(0.0f, svec.get(3), 0.0f);

        agg.aggregateSpatial(ctx, obs(4, 7.3f, 0.5f, Float.NaN), svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(4, svec.get(1), 0.0f);
        assertEquals(7.3f, svec.get(2), 0.0f);
        assertEquals(0.5f, svec.get(3), 0.0f);

        agg.completeSpatial(ctx, 1, svec);
        assertEquals(NaN, svec.get(0), 0.0f);
        assertEquals(NaN, svec.get(1), 0.0f);
        assertEquals(NaN, svec.get(2), 0.0f);
        assertEquals(NaN, svec.get(3), 0.0f);

        agg.initTemporal(ctx, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(NaN, tvec.get(1), 0.0f);
        assertEquals(NaN, tvec.get(2), 0.0f);
        assertEquals(NaN, tvec.get(3), 0.0f);

        agg.aggregateTemporal(ctx, vec(Float.NaN, 4, 0.2f, 9.7f), 3, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(NaN, tvec.get(1), 0.0f);
        assertEquals(NaN, tvec.get(2), 0.0f);
        assertEquals(NaN, tvec.get(3), 0.0f);
        agg.completeTemporal(ctx, 1, tvec);

        agg.computeOutput(tvec, out);
        assertEquals(NaN, out.get(0), 0.0f);
        assertEquals(NaN, out.get(1), 0.0f);
        assertEquals(NaN, out.get(2), 0.0f);
        assertEquals(NaN, out.get(3), 0.0f);
    }


    public static VectorImpl vec(float... values) {
        return new VectorImpl(values);
    }

    public static Observation obs(double mjd, float... values) {
        return new ObservationImpl(0.0, 0.0, mjd, values);
    }

    private static BinContext createCtx() {
        return new TestBinContext();
    }

    static class MyVariableContext implements VariableContext {
        private String[] varNames;

        public MyVariableContext(String... varNames) {
            this.varNames = varNames;
        }

        @Override
        public int getVariableCount() {
            return varNames.length;
        }

        @Override
        public String getVariableName(int i) {
            return varNames[i];
        }

        @Override
        public int getVariableIndex(String name) {
            for (int i = 0; i < varNames.length; i++) {
                if (name.equals(varNames[i])) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public String getVariableExpression(int i) {
            return null;
        }

        @Override
        public String getVariableValidExpression(int index) {
            return null;
        }

        @Override
        public String getValidMaskExpression() {
            return null;
        }
    }
}