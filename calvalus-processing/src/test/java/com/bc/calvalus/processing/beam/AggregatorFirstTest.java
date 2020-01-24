package com.bc.calvalus.processing.beam;

import org.esa.snap.binning.BinContext;
import org.esa.snap.binning.Observation;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.support.ObservationImpl;
import org.esa.snap.binning.support.VectorImpl;
import org.junit.Before;
import org.junit.Test;

import static java.lang.Float.NaN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class AggregatorFirstTest {
    private BinContext ctx;
    private AggregatorFirst agg;

    @Before
    public void setUp() {
        ctx = createCtx();
        agg = new AggregatorFirst(new MyVariableContext("a", "b", "c"),"c", "Out");
    }

    @Test
    public void testRequiresGrowableSpatialData() {
        assertFalse(agg.requiresGrowableSpatialData());
    }

    @Test
    public void testMetadata() {
        assertEquals("FIRST", agg.getName());

        assertEquals(1, agg.getSpatialFeatureNames().length);
        assertEquals("c", agg.getSpatialFeatureNames()[0]);

        assertEquals(1, agg.getTemporalFeatureNames().length);
        assertEquals("c", agg.getTemporalFeatureNames()[0]);

        assertEquals(1, agg.getOutputFeatureNames().length);
        assertEquals("Out", agg.getOutputFeatureNames()[0]);
    }

    @Test
    public void testAggregatorOnMaxSet() {
        VectorImpl svec = vec(NaN);
        VectorImpl tvec = vec(NaN);
        VectorImpl out = vec(NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(NaN, svec.get(0), 0.0f);

        agg.aggregateSpatial(ctx, obs(4, 0.99f, 0.88f, 7.3f), svec);
        agg.aggregateSpatial(ctx, obs(5, 0.99f, 0.88f, 0.1f), svec);
        agg.aggregateSpatial(ctx, obs(6, 0.99f, 0.88f, 5.5f), svec);
        assertEquals(7.3f, svec.get(0), 1e-5f);

        agg.completeSpatial(ctx, 3, svec);
        assertEquals(7.3f, svec.get(0), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(NaN, tvec.get(0), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.3f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(1.1f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(4.7f), 3, tvec);
        agg.completeTemporal(ctx, 3, tvec);
        assertEquals(0.3f, tvec.get(0), 1e-5f);

        agg.computeOutput(tvec, out);
        assertEquals(0.3f, out.get(0), 1e-5f);
    }

    @Test
    public void testAggregatorFirst_AllNaN() {
        VectorImpl svec = vec(NaN);
        VectorImpl tvec = vec(NaN);
        VectorImpl out = vec(NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(NaN, svec.get(0), 0.0f);

        agg.aggregateSpatial(ctx, obs(4, .099f, 0.88f, Float.NaN), svec);
        assertEquals(NaN, svec.get(0), 0.0f);

        agg.completeSpatial(ctx, 1, svec);
        assertEquals(NaN, svec.get(0), 0.0f);

        agg.initTemporal(ctx, tvec);
        assertEquals(NaN, tvec.get(0), 0.0f);

        agg.aggregateTemporal(ctx, vec(Float.NaN, 4, 0.2f, 9.7f), 3, tvec);
        assertEquals(NaN, tvec.get(0), 0.0f);
        agg.completeTemporal(ctx, 1, tvec);

        agg.computeOutput(tvec, out);
        assertEquals(NaN, out.get(0), 0.0f);
    }


    public static VectorImpl vec(float... values) {
        return new VectorImpl(values);
    }

    public static Observation obs(double mjd, float... values) {
        return new ObservationImpl(0.0, 0.0, mjd, values);
    }

    public static BinContext createCtx() {
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