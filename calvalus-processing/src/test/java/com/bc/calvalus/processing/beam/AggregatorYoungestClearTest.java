package com.bc.calvalus.processing.beam;

import org.esa.snap.binning.BinContext;
import org.esa.snap.binning.Observation;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.support.ObservationImpl;
import org.esa.snap.binning.support.VectorImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static java.lang.Float.NaN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class AggregatorYoungestClearTest {
    private BinContext ctx;
    private AggregatorYoungestClear agg;

    @Before
    public void setUp() {
        ctx = createCtx();
        agg = new AggregatorYoungestClear(new MyVariableContext("a", "b", "c", "d"),
                                          "b", 2066, 8, 50,
                                          "c", 25, 75,
                                          "age", 33,"a", "c", "d");
    }

    @Test
    public void testRequiresGrowableSpatialData() {
        assertFalse(agg.requiresGrowableSpatialData());
    }

    @Test
    public void testMetadata() {
        assertEquals("YOUNGEST_CLEAR", agg.getName());

        assertEquals(6, agg.getSpatialFeatureNames().length);
        assertEquals("a", agg.getSpatialFeatureNames()[0]);
        assertEquals("c", agg.getSpatialFeatureNames()[1]);
        assertEquals("d", agg.getSpatialFeatureNames()[2]);
        assertEquals("age", agg.getSpatialFeatureNames()[3]);
        assertEquals("b", agg.getSpatialFeatureNames()[4]);
        assertEquals("c", agg.getSpatialFeatureNames()[5]);

        assertEquals(6, agg.getTemporalFeatureNames().length);
        assertEquals("a", agg.getTemporalFeatureNames()[0]);
        assertEquals("c", agg.getTemporalFeatureNames()[1]);
        assertEquals("d", agg.getTemporalFeatureNames()[2]);
        assertEquals("age", agg.getTemporalFeatureNames()[3]);
        assertEquals("b", agg.getTemporalFeatureNames()[4]);
        assertEquals("c", agg.getTemporalFeatureNames()[5]);

        assertEquals(4, agg.getOutputFeatureNames().length);
        assertEquals("a", agg.getOutputFeatureNames()[1]);
        assertEquals("c", agg.getOutputFeatureNames()[2]);
        assertEquals("d", agg.getOutputFeatureNames()[3]);
        assertEquals("age", agg.getOutputFeatureNames()[0]);
    }

    @Test
    public void testAggregatorYoungest() {
        VectorImpl svec = vec(NaN, NaN, NaN, NaN, NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN, NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN, NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(Float.NaN, svec.get(0), 0.0f);

        agg.aggregateSpatial(ctx, obs(6, 0.99f, 0, 0.81f, 5.5f), svec);
        agg.aggregateSpatial(ctx, obs(4, 0.99f, 16, 0.85f, NaN), svec);
        agg.aggregateSpatial(ctx, obs(5, 0.99f, 24, 0.89f, 0.1f), svec);
        assertEquals(5.5f, svec.get(2), 1e-5f);

        agg.completeSpatial(ctx, 3, svec);
        assertEquals(5.5f, svec.get(2), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(NaN, tvec.get(0), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.91f, 0.3f, 0.81f, 6f, 2f, 0.3f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.92f, 0.1f, 0.82f, 8f, 2f, 0.1f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.93f, 0.5f, 0.83f, 7f, 10f, 0.5f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.99f, 0.7f, 0.84f, 5f, 0f, 0.7f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.95f, 0.6f, 0.85f, 4f, 0f, 0.6f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.96f, 0.9f, 0.86f, 9f, 2f, 0.9f), 1, tvec);
        agg.completeTemporal(ctx, 6, tvec);
        assertEquals(0.91f, tvec.get(0), 1e-5f);
        assertEquals(6, tvec.get(3), 1e-5f);

        agg.computeOutput(tvec, out);
        assertEquals(27, out.get(0), 1e-5f);
        assertEquals(0.91f, out.get(1), 1e-5f);
        assertEquals(0.3f, out.get(2), 1e-5f);
        assertEquals(0.81f, out.get(3), 1e-5f);
    }

    @Test
    public void testAggregatorYoungest_nofallback() {
        agg = new AggregatorYoungestClear(new MyVariableContext("a", "b", "c", "d"),
                                          "b", 2059, 9, 10,
                                          "c", 10, 80,
                                          "age", 33,"a", "c", "d");

        VectorImpl tvec = vec(NaN, NaN, NaN, NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN, NaN, NaN);

        agg.initTemporal(ctx, tvec);
        assertEquals(NaN, tvec.get(0), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.91f, 0.3f, 0.81f, 6f, 10f, 0.3f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.92f, 0.1f, 0.82f, 8f, 10f, 0.1f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.93f, 0.5f, 0.83f, 7f, 8f, 0.5f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.99f, 0.7f, 0.84f, 5f, 0f, 0.7f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.95f, 0.6f, 0.85f, 4f, 10f, 0.6f), 1, tvec);
        agg.completeTemporal(ctx, 5, tvec);
        assertEquals(0.99f, tvec.get(0), 1e-5f);
        assertEquals(5, tvec.get(3), 1e-5f);

        agg.computeOutput(tvec, out);
        assertEquals(28, out.get(0), 1e-5f);
        assertEquals(0.99f, out.get(1), 1e-5f);
        assertEquals(0.7f, out.get(2), 1e-5f);
        assertEquals(0.84f, out.get(3), 1e-5f);
    }

    @Test
    public void testAggregatorYoungest_AllNaN() {
        VectorImpl svec = vec(NaN, NaN, NaN, NaN, NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN, NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN, NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(NaN, svec.get(0), 0.0f);

        agg.aggregateSpatial(ctx, obs(4, 0.99f, 16, 0.85f, NaN), svec);
        assertEquals(NaN, svec.get(2), 0.0f);

        agg.completeSpatial(ctx, 1, svec);
        assertEquals(NaN, svec.get(2), 0.0f);

        agg.initTemporal(ctx, tvec);
        assertEquals(NaN, tvec.get(0), 0.0f);

        agg.aggregateTemporal(ctx, vec(Float.NaN, 0.3f, 0.81f, 6f, 2f, 0.3f), 1, tvec);
        assertEquals(NaN, tvec.get(0), 0.0f);
        agg.completeTemporal(ctx, 1, tvec);

        agg.computeOutput(tvec, out);
        assertEquals(NaN, out.get(1), 0.0f);
    }

    @Test
    public void testAggregatorYoungest_differentSequences() {
        VectorImpl tvec = vec(NaN, NaN, NaN, NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN, NaN, NaN);

        VectorImpl[] vectors = {
            vec(0.91f, 0.3f, 0.81f, 6f, 2f, 0.3f),
            vec(0.92f, 0.1f, 0.82f, 8f, 2f, 0.1f),
            vec(0.93f, 0.5f, 0.83f, 7f, 10f, 0.5f),
            vec(0.99f, 0.7f, 0.84f, 5f, 0f, 0.7f),
            vec(0.95f, 0.6f, 0.85f, 4f, 0f, 0.6f),
            vec(0.96f, 0.9f, 0.86f, 9f, 2f, 0.9f)
        };

        permute(vectors, 0, tvec, out);
    }

    void testAggregatorYoungest(VectorImpl[] svecs, VectorImpl tvec, VectorImpl out) {
        agg.initTemporal(ctx, tvec);
        for (VectorImpl svec : svecs) {
            agg.aggregateTemporal(ctx, svec, 1, tvec);
        }
        agg.completeTemporal(ctx, 6, tvec);
        agg.computeOutput(tvec, out);
        assertEquals(27, out.get(0), 1e-5f);
        assertEquals(0.91f, out.get(1), 1e-5f);
        assertEquals(0.3f, out.get(2), 1e-5f);
        assertEquals(0.81f, out.get(3), 1e-5f);

    }

    void permute(VectorImpl[] arr, int k, VectorImpl tvec, VectorImpl out) {
        for(int i = k; i < arr.length; i++){
            swap(arr, i, k);
            permute(arr, k+1, tvec, out);
            swap(arr, k, i);
        }
        if (k == arr.length-1){
            testAggregatorYoungest(arr, tvec, out);
        }
    }

    static void swap(VectorImpl[] vec, int i, int k) {
        VectorImpl v = vec[i];
        vec[i] = vec[k];
        vec[k] = v;
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